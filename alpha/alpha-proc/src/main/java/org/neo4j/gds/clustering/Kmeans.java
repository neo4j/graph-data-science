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
package org.neo4j.gds.clustering;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;

public class Kmeans extends Algorithm<HugeLongArray> {
    private final HugeLongArray inCluster;
    private final Graph graph;
    private final KmeansBaseConfig config;
    private final KmeansContext context;
    private final SplittableRandom splittableRandom;
    private final HugeObjectArray<double[]> nodeProperties;
    public static final long UNASSIGNED = -1;

    public static Kmeans createKmeans(Graph graph, KmeansBaseConfig config, KmeansContext context) {
        String nodeWeightProperty = config.nodeWeightProperty();
        NodeProperties nodePropertiesAll = graph.nodeProperties(nodeWeightProperty);
        HugeObjectArray<double[]> nodeProperties = HugeObjectArray.newArray(double[].class, graph.nodeCount());
        graph.forEachNode(nodeId -> {
            nodeProperties.set(nodeId, nodePropertiesAll.doubleArrayValue(nodeId));
            return true;
        });
        return new Kmeans(
            context.progressTracker(),
            graph,
            config,
            context,
            nodeProperties,
            getSplittableRandom(config.randomSeed())
        );
    }

    Kmeans(
        ProgressTracker progressTracker,
        Graph graph,
        KmeansBaseConfig config,
        KmeansContext context,
        HugeObjectArray<double[]> nodeProperties,
        SplittableRandom splittableRandom
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.context = context;
        this.splittableRandom = splittableRandom;
        this.inCluster = HugeLongArray.newArray(graph.nodeCount());
        this.nodeProperties = nodeProperties;
    }

    @Override
    public HugeLongArray compute() {
        int numberOfDimensions = nodeProperties.get(0).length;
        int maxIterations = config.maxIterations();
        int K = config.K();
        if (K > graph.nodeCount()) {
            K = (int) graph.nodeCount();
            progressTracker.logWarning("Number of requested clusters is larger than the number of nodes.");
            inCluster.setAll(v -> v);
            return inCluster;
        }
        long nodeCount = graph.nodeCount();
        int concurrency = config.concurrency();
        double[][] clusterCentre = new double[K][numberOfDimensions];
        inCluster.setAll(v -> UNASSIGNED);

        int finalK = K;
        var kmeansThreads = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new KmeansThread(
                clusterCentre,
                nodeProperties,
                inCluster,
                finalK,
                numberOfDimensions,
                partition,
                progressTracker
            ),
            Optional.of(config.minBatchSize())
        );
        int numberOfTasks = kmeansThreads.size();
        //Initialization do initial centre computation and assignment
        //Temporary:
        KmeansSampler sampler = new KmeansUniformSampler();
        List<Long> initialCentreIds = sampler.sampleClusters(splittableRandom, nodeProperties, nodeCount, K);
        assignCentres(clusterCentre, initialCentreIds, numberOfDimensions);
        //
        for (int iteration = 0; iteration < maxIterations; ++iteration) {
            long swaps = 0;
            //assign each node to a centre

            ParallelUtil.runWithConcurrency(concurrency, kmeansThreads, context.executor());

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
        int K = clusterCentre.length;
        int numberOfDimensions = clusterCentre[0].length;
        long[] nodesInCluster = new long[K];
        for (int centreId = 0; centreId < K; ++centreId) {
            for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                clusterCentre[centreId][dimension] = 0.0;
            }
        }
        for (KmeansThread thread : kmeansThreads) {
            for (int centreId = 0; centreId < K; ++centreId) {
                var centreContribution = thread.getCentreContribution(centreId);
                nodesInCluster[centreId] += thread.getNumAssignedAtCentre(centreId);
                for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                    clusterCentre[centreId][dimension] += centreContribution[dimension];
                }
            }
        }
        for (int centreId = 0; centreId < K; ++centreId) {
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
            var property = nodeProperties.get(centreId);
            for (int j = 0; j < numberOfDimensions; ++j) {
                clusterCentre[clusterUpdateId][j] = property[j];
            }
            clusterUpdateId++;
        }
    }

    private static final class KmeansThread implements Runnable {

        private final ProgressTracker progressTracker;
        private final Partition partition;
        private double[][] centreSumAtDimension;
        private HugeObjectArray<double[]> nodeProperties;
        private HugeLongArray inCluster;
        private long[] numAssignedAtCentre;
        private double[][] clusterCentre;
        private long swaps;
        private int K;
        private int numberOfDimensions;

        public KmeansThread(
            double[][] clusterCentre,
            HugeObjectArray<double[]> nodeProperties,
            HugeLongArray inCluster,
            int K,
            int numberOfDimensions,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.progressTracker = progressTracker;
            this.partition = partition;
            this.clusterCentre = clusterCentre;
            this.centreSumAtDimension = new double[K][numberOfDimensions];
            this.numAssignedAtCentre = new long[K];
            this.K = K;
            this.numberOfDimensions = numberOfDimensions;
            this.nodeProperties = nodeProperties;
            this.inCluster = inCluster;
        }

        public double[] getCentreContribution(int ith) {
            return centreSumAtDimension[ith];
        }

        public long getNumAssignedAtCentre(int ith) {
            return numAssignedAtCentre[ith];
        }

        public long getSwaps() {
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
            for (int centreId = 0; centreId < K; ++centreId) {
                numAssignedAtCentre[centreId] = 0;
                for (int dimension = 0; dimension < numberOfDimensions; ++dimension) {
                    centreSumAtDimension[centreId][dimension] = 0.0;
                }
            }
            for (long nodeId = startNode; nodeId < endNode; nodeId++) {
                int bestPosition = 0;
                var nodePropertyArray = nodeProperties.get(nodeId);
                double smallestDistance = euclidean(nodePropertyArray, clusterCentre[0]);
                for (int centreId = 1; centreId < K; ++centreId) {
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

