/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.kmeans;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.utils.StringFormatting;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;

public class Kmeans extends Algorithm<HugeIntArray> {

    private final HugeIntArray communities;
    private final Graph graph;
    private final int k;
    private final int concurrency;
    private final int maxIterations;
    private final ExecutorService executorService;
    private final SplittableRandom random;
    private final NodeProperties nodeProperties;

    private static final int UNASSIGNED = -1;

    public static Kmeans createKmeans(Graph graph, KmeansBaseConfig config, KmeansContext context) {
        String nodeWeightProperty = config.nodeWeightProperty();
        NodeProperties nodeProperties = graph.nodeProperties(nodeWeightProperty);
        return new Kmeans(
            context.progressTracker(),
            context.executor(),
            graph,
            config.k(),
            config.concurrency(),
            config.maxIterations(),
            nodeProperties,
            getSplittableRandom(config.randomSeed())
        );
    }

    private static void validateNodeProperties(NodeProperties nodeProperties) {
        var valueType = nodeProperties.valueType();
        if (valueType == ValueType.DOUBLE_ARRAY || valueType == ValueType.FLOAT_ARRAY) {
            return;
        }
        throw new IllegalArgumentException(
            StringFormatting.formatWithLocale(
                "Unsupported node property value type [%s]. Value type required: [%s] or [%s].",
                valueType,
                ValueType.DOUBLE_ARRAY,
                ValueType.FLOAT_ARRAY
            )
        );
    }

    Kmeans(
        ProgressTracker progressTracker,
        ExecutorService executorService,
        Graph graph,
        int k,
        int concurrency,
        int maxIterations,
        NodeProperties nodeProperties,
        SplittableRandom random
    ) {
        super(progressTracker);
        this.executorService = executorService;
        this.graph = graph;
        this.k = k;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.random = random;
        this.communities = HugeIntArray.newArray(graph.nodeCount());
        validateNodeProperties(nodeProperties);
        this.nodeProperties = nodeProperties;
    }

    @Override
    public HugeIntArray compute() {
        int propertyDimensions = nodeProperties.doubleArrayValue(0).length;
        if (k > graph.nodeCount()) {
            // Every node in its own community. Warn and return early.
            progressTracker.logWarning("Number of requested clusters is larger than the number of nodes.");
            communities.setAll(v -> (int) v);
            return communities;
        }
        long nodeCount = graph.nodeCount();
        double[][] clusterCenters = new double[k][propertyDimensions];
        communities.setAll(v -> UNASSIGNED);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new KmeansTask(
                clusterCenters,
                nodeProperties,
                communities,
                k,
                propertyDimensions,
                partition,
                progressTracker
            ),
            Optional.of((int) nodeCount / concurrency)
        );
        int numberOfTasks = tasks.size();
        
        assert numberOfTasks <= concurrency;

        //Initialization do initial center computation and assignment
        //Temporary:
        KmeansSampler sampler = new KmeansUniformSampler();
        List<Long> initialCenterIds = sampler.sampleClusters(random, nodeProperties, nodeCount, k);
        assignCenters(clusterCenters, initialCenterIds, propertyDimensions);
        //
        for (int iteration = 0; iteration < maxIterations; ++iteration) {
            long swaps = 0;
            //assign each node to a center
            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

            for (int threadId = 0; threadId < numberOfTasks; ++threadId) {
                swaps += tasks.get(threadId).getSwaps();
            }
            if (swaps == 0) {
                break;
            }
            recomputeCenters(clusterCenters, tasks);
        }
        return communities;
    }

    private void recomputeCenters(double[][] clusterCenters, List<KmeansTask> tasks) {
        int propertyDimensions = clusterCenters[0].length;
        long[] nodesInCluster = new long[k];
        for (int centerId = 0; centerId < k; ++centerId) {
            for (int dimension = 0; dimension < propertyDimensions; ++dimension) {
                clusterCenters[centerId][dimension] = 0.0;
            }
        }
        for (KmeansTask task : tasks) {
            for (int centerId = 0; centerId < k; ++centerId) {
                var centerContribution = task.getCenterContribution(centerId);
                nodesInCluster[centerId] += task.getNumAssignedAtCenter(centerId);
                for (int dimension = 0; dimension < propertyDimensions; ++dimension) {
                    clusterCenters[centerId][dimension] += centerContribution[dimension];
                }
            }
        }
        for (int centerId = 0; centerId < k; ++centerId) {
            for (int dimension = 0; dimension < propertyDimensions; ++dimension) {
                clusterCenters[centerId][dimension] /= (double) nodesInCluster[centerId];
            }
        }

    }

    @Override
    public void release() {

    }

    @NotNull
    private static SplittableRandom getSplittableRandom(Optional<Long> randomSeed) {
        return randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
    }

    private void assignCenters(double[][] clusterCenters, List<Long> initialCenterIds, int dimentions) {
        int clusterUpdateId = 0;
        for (long centerId : initialCenterIds) {
            var property = nodeProperties.doubleArrayValue(centerId);
            for (int j = 0; j < dimentions; ++j) {
                clusterCenters[clusterUpdateId][j] = property[j];
            }
            clusterUpdateId++;
        }
    }

    private static final class KmeansTask implements Runnable {

        private final ProgressTracker progressTracker;
        private final Partition partition;
        private final double[][] centerSumAtDimension;
        private final NodeProperties nodeProperties;
        private final HugeIntArray communities;
        private final long[] numAssignedAtCenter;
        private final double[][] clusterCenters;
        private final int k;
        private final int numberOfDimensions;

        private long swaps;

        KmeansTask(
            double[][] clusterCenters,
            NodeProperties nodeProperties,
            HugeIntArray communities,
            int k,
            int propertyDimensions,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.progressTracker = progressTracker;
            this.partition = partition;
            this.clusterCenters = clusterCenters;
            this.centerSumAtDimension = new double[k][propertyDimensions];
            this.numAssignedAtCenter = new long[k];
            this.k = k;
            this.numberOfDimensions = propertyDimensions;
            this.nodeProperties = nodeProperties;
            this.communities = communities;
        }

        double[] getCenterContribution(int ith) {
            return centerSumAtDimension[ith];
        }

        long getNumAssignedAtCenter(int ith) {
            return numAssignedAtCenter[ith];
        }

        long getSwaps() {
            return swaps;
        }

        private double euclidean(double[] left, double[] right) {
            return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));
        }

        @Override
        public void run() {
            var startNode = partition.startNode();
            long endNode = startNode + partition.nodeCount();
            swaps = 0;
            for (int centerId = 0; centerId < k; ++centerId) {
                numAssignedAtCenter[centerId] = 0;
                for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                    centerSumAtDimension[centerId][dimension] = 0.0;
                }
            }
            for (long nodeId = startNode; nodeId < endNode; nodeId++) {
                int bestPosition = 0;
                var nodePropertyArray = nodeProperties.doubleArrayValue(nodeId);
                double smallestDistance = euclidean(nodePropertyArray, clusterCenters[0]);
                for (int centerId = 1; centerId < k; ++centerId) {
                    double tempDistance = euclidean(nodePropertyArray, clusterCenters[centerId]);
                    if (tempDistance < smallestDistance) {
                        smallestDistance = tempDistance;
                        bestPosition = centerId;
                    }
                }
                numAssignedAtCenter[bestPosition]++;
                int previousCluster = communities.get(nodeId);
                if (bestPosition != previousCluster) {
                    swaps++;
                }
                //Note for potential improvement : This is potentially costly when clusters have somewhat stabilized.
                //Because we keep adding the same nodes to the same clusters. Perhaps instead of making the sum 0
                //we keep as is and do a subtraction when a node changes its cluster.
                //On that note,  maybe we can skip stable communities (i.e., communities that did not change between one iteration to another)
                // or avoid calculating their distance from other nodes etc...
                communities.set(nodeId, bestPosition);
                for (int j = 0; j < numberOfDimensions; ++j) {
                    centerSumAtDimension[bestPosition][j] += nodePropertyArray[j];
                }
            }
        }
    }
}
