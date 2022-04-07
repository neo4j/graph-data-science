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
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.utils.StringFormatting;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;

public class Kmeans extends Algorithm<HugeLongArray> {

    private final HugeLongArray inCluster;
    private final Graph graph;
    private final int k;
    private final int concurrency;
    private final int maxIterations;
    private final ExecutorService executorService;
    private final SplittableRandom splittableRandom;
    private final NodeProperties nodeProperties;

    private static final long UNASSIGNED = -1;

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
        SplittableRandom splittableRandom
    ) {
        super(progressTracker);
        this.executorService = executorService;
        this.graph = graph;
        this.k = k;
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.splittableRandom = splittableRandom;
        this.inCluster = HugeLongArray.newArray(graph.nodeCount());
        validateNodeProperties(nodeProperties);
        this.nodeProperties = nodeProperties;
    }

    @Override
    public HugeLongArray compute() {
        int numberOfDimensions = nodeProperties.doubleArrayValue(0).length;
        if (k > graph.nodeCount()) {
            // Every node in its own community. Warn and return early.
            progressTracker.logWarning("Number of requested clusters is larger than the number of nodes.");
            inCluster.setAll(v -> v);
            return inCluster;
        }
        long nodeCount = graph.nodeCount();
        double[][] clusterCentre = new double[k][numberOfDimensions];
        inCluster.setAll(v -> UNASSIGNED);

        var kmeansThreads = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new KmeansThread(
                clusterCentre,
                nodeProperties,
                inCluster,
                k,
                numberOfDimensions,
                partition,
                progressTracker
            ),
            Optional.of((int) nodeCount / concurrency)
        );
        int numberOfTasks = kmeansThreads.size();
        
        assert numberOfTasks <= concurrency;

        //Initialization do initial centre computation and assignment
        //Temporary:
        KmeansSampler sampler = new KmeansUniformSampler();
        List<Long> initialCentreIds = sampler.sampleClusters(splittableRandom, nodeProperties, nodeCount, k);
        assignCentres(clusterCentre, initialCentreIds, numberOfDimensions);
        //
        for (int iteration = 0; iteration < maxIterations; ++iteration) {
            long swaps = 0;
            //assign each node to a centre
            ParallelUtil.runWithConcurrency(concurrency, kmeansThreads, executorService);

            for (int threadId = 0; threadId < numberOfTasks; ++threadId) {
                swaps += kmeansThreads.get(threadId).getSwaps();
            }
            if (swaps == 0) {
                break;
            }
            recomputeCenters(clusterCentre, kmeansThreads);
        }
        return inCluster;
    }

    private void recomputeCenters(double[][] clusterCentre, List<KmeansThread> kmeansThreads) {
        int k = clusterCentre.length;
        int numberOfDimensions = clusterCentre[0].length;
        long[] nodesInCluster = new long[k];
        for (int centreId = 0; centreId < k; ++centreId) {
            for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                clusterCentre[centreId][dimension] = 0.0;
            }
        }
        for (KmeansThread thread : kmeansThreads) {
            for (int centreId = 0; centreId < k; ++centreId) {
                var centreContribution = thread.getCentreContribution(centreId);
                nodesInCluster[centreId] += thread.getNumAssignedAtCentre(centreId);
                for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                    clusterCentre[centreId][dimension] += centreContribution[dimension];
                }
            }
        }
        for (int centreId = 0; centreId < k; ++centreId) {
            for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                clusterCentre[centreId][dimension] /= (double) nodesInCluster[centreId];
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

    private void assignCentres(double[][] clusterCentre, List<Long> initialCentreIds, int numberOfDimensions) {
        int clusterUpdateId = 0;
        for (long centreId : initialCentreIds) {
            var property = nodeProperties.doubleArrayValue(centreId);
            for (int j = 0; j < numberOfDimensions; ++j) {
                clusterCentre[clusterUpdateId][j] = property[j];
            }
            clusterUpdateId++;
        }
    }

    private static final class KmeansThread implements Runnable {

        private final ProgressTracker progressTracker;
        private final Partition partition;
        private final double[][] centreSumAtDimension;
        private final NodeProperties nodeProperties;
        private final HugeLongArray inCluster;
        private final long[] numAssignedAtCentre;
        private final double[][] clusterCentre;
        private final int k;
        private final int numberOfDimensions;

        private long swaps;

        KmeansThread(
            double[][] clusterCentre,
            NodeProperties nodeProperties,
            HugeLongArray inCluster,
            int k,
            int numberOfDimensions,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.progressTracker = progressTracker;
            this.partition = partition;
            this.clusterCentre = clusterCentre;
            this.centreSumAtDimension = new double[k][numberOfDimensions];
            this.numAssignedAtCentre = new long[k];
            this.k = k;
            this.numberOfDimensions = numberOfDimensions;
            this.nodeProperties = nodeProperties;
            this.inCluster = inCluster;
        }

        double[] getCentreContribution(int ith) {
            return centreSumAtDimension[ith];
        }

        long getNumAssignedAtCentre(int ith) {
            return numAssignedAtCentre[ith];
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
            for (int centreId = 0; centreId < k; ++centreId) {
                numAssignedAtCentre[centreId] = 0;
                for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                    centreSumAtDimension[centreId][dimension] = 0.0;
                }
            }
            for (long nodeId = startNode; nodeId < endNode; nodeId++) {
                int bestPosition = 0;
                var nodePropertyArray = nodeProperties.doubleArrayValue(nodeId);
                double smallestDistance = euclidean(nodePropertyArray, clusterCentre[0]);
                for (int centreId = 1; centreId < k; ++centreId) {
                    double tempDistance = euclidean(nodePropertyArray, clusterCentre[centreId]);
                    if (tempDistance < smallestDistance) {
                        smallestDistance = tempDistance;
                        bestPosition = centreId;
                    }
                }
                numAssignedAtCentre[bestPosition]++;
                int previousCluster = (int) inCluster.get(nodeId);
                if (bestPosition != previousCluster) {
                    swaps++;
                }
                //Note for potential improvement : This is potentially costly when clusters have somewhat stabilized.
                //Because we keep adding the same nodes to the same clusters. Perhaps instead of making the sum 0
                //we keep as is and do a subtraction when a node changes its cluster.
                //On that note,  maybe we can skip stable communities (i.e., communities that did not change between one iteration to another)
                // or avoid calculating their distance from other nodes etc...
                inCluster.set(nodeId, bestPosition);
                for (int j = 0; j < numberOfDimensions; ++j) {
                    centreSumAtDimension[bestPosition][j] += nodePropertyArray[j];
                }
            }
        }
    }
}
