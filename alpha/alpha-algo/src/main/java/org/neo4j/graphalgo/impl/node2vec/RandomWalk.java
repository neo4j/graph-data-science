/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.node2vec;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.queue.QueueBasedSpliterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.PrimitiveIterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class RandomWalk extends Algorithm<RandomWalk, Stream<long[]>> {

    private final Graph graph;
    private final int steps;
    private final NextNodeStrategy strategy;
    private final int concurrency;
    private final int walksPerNode;

    public RandomWalk(
        Graph graph,
        int steps,
        NextNodeStrategy strategy,
        int concurrency,
        int walksPerNode
    ) {
        this.graph = graph;
        this.steps = steps;
        this.strategy = strategy;
        this.concurrency = concurrency;
        this.walksPerNode = walksPerNode;
    }

    @Override
    public Stream<long[]> compute() {
        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustedBatchSize(walksPerNode, concurrency, 100);
        Collection<Runnable> tasks = new ArrayList<>((walksPerNode / batchSize) + 1);

        ArrayBlockingQueue<long[]> queue = new ArrayBlockingQueue<>(queueSize);
        long[] TOMB = new long[0];

        PrimitiveIterator.OfLong idIterator = idStream().iterator();
        while (idIterator.hasNext()) {
            long[] ids = new long[batchSize];
            int i = 0;
            while (i < batchSize && idIterator.hasNext()) {
                ids[i++] = idIterator.nextLong();
            }
            int size = i;
            tasks.add(() -> {
                for (int j = 0; j < size; j++) {
                    put(queue, doWalk(ids[j]));
                }
            });
        }
        new Thread(() -> {
            ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            put(queue, TOMB);
        }).start();

        QueueBasedSpliterator<long[]> spliterator = new QueueBasedSpliterator<>(queue, TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public RandomWalk me() {
        return this;
    }

    @Override
    public void release() { }

    @NotNull
    public LongStream idStream() {
        LongStream idStream = LongStream.of();
        for (int i = 0; i < walksPerNode; i++) {
            idStream = LongStream.concat(idStream, LongStream.range(0, graph.nodeCount()));
        }
        return idStream;
    }

    private long[] doWalk(long startNodeId) {
        long[] nodeIds = new long[steps + 1];
        long currentNodeId = startNodeId;
        long previousNodeId = currentNodeId;
        nodeIds[0] = toOriginalNodeId(currentNodeId);
        for (int i = 1; i <= steps; i++) {
            long nextNodeId = strategy.getNextNode(currentNodeId, previousNodeId);
            previousNodeId = currentNodeId;
            currentNodeId = nextNodeId;

            if (currentNodeId == -1 || !terminationFlag.running()) {
                // End walk when there is no way out and return empty result
                return Arrays.copyOf(nodeIds, 1);
            }
            nodeIds[i] = toOriginalNodeId(currentNodeId);
        }

        return nodeIds;
    }

    private long toOriginalNodeId(long currentNodeId) {
        return currentNodeId == -1 ? -1 : graph.toOriginalNodeId(currentNodeId);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {}
    }

    public static class NextNodeStrategy {
        private final Graph graph;
        private final double returnParam;
        private final double inOutParam;


        public NextNodeStrategy(Graph graph, double returnParam, double inOutParam) {
            this.graph = graph;
            this.returnParam = returnParam;
            this.inOutParam = inOutParam;
        }

        public long getNextNode(long currentNode, long previousNode) {
            Graph threadLocalGraph = graph.concurrentCopy();

            int degree = threadLocalGraph.degree(currentNode);
            if (degree == 0) {
                return -1;
            }

            double[] distribution = buildProbabilityDistribution(
                threadLocalGraph,
                currentNode,
                previousNode,
                returnParam,
                inOutParam,
                degree
            );
            int neighbourIndex = pickIndexFromDistribution(distribution, ThreadLocalRandom.current().nextDouble());

            return threadLocalGraph.getTarget(currentNode, neighbourIndex);
        }

        private double[] buildProbabilityDistribution(
            Graph threadLocalGraph,
            long currentNodeId,
            long previousNodeId,
            double returnParam,
            double inOutParam,
            int degree
        ) {
            ProbabilityDistributionComputer consumer = new ProbabilityDistributionComputer(
                threadLocalGraph.concurrentCopy(),
                degree,
                currentNodeId,
                previousNodeId,
                returnParam,
                inOutParam
            );
            threadLocalGraph.forEachRelationship(currentNodeId, consumer);
            return consumer.probabilities();
        }

        private static double[] normalizeDistribution(double[] array, double sum) {
            for (int i = 0; i < array.length; i++) {
                array[i] /= sum;
            }
            return array;
        }

        private static int pickIndexFromDistribution(double[] distribution, double probability) {
            double cumulativeProbability = 0.0;
            for (int i = 0; i < distribution.length; i++) {
                cumulativeProbability += distribution[i];
                if (probability <= cumulativeProbability) {
                    return i;
                }
            }
            return distribution.length - 1;
        }

        private class ProbabilityDistributionComputer implements RelationshipConsumer {
            private final Graph threadLocalGraph;
            final double[] probabilities;
            private final long currentNodeId;
            private final long previousNodeId;
            private final double returnParam;
            private final double inOutParam;
            double probSum;
            int index;

            public ProbabilityDistributionComputer(
                Graph threadLocalGraph,
                int degree,
                long currentNodeId,
                long previousNodeId,
                double returnParam,
                double inOutParam
            ) {
                this.threadLocalGraph = threadLocalGraph;
                this.currentNodeId = currentNodeId;
                this.previousNodeId = previousNodeId;
                this.returnParam = returnParam;
                this.inOutParam = inOutParam;
                probabilities = new double[degree];
                probSum = 0;
                index = 0;
            }

            @Override
            public boolean accept(long start, long end) {
                long neighbourId = start == currentNodeId ? end : start;

                double probability;

                if (neighbourId == previousNodeId) {
                    // node is previous node
                    probability = 1D / returnParam;
                } else if (threadLocalGraph.exists(previousNodeId, neighbourId)) {
                    // node is also adjacent to previous node --> distance to previous node is 1
                    probability = 1D;
                } else {
                    // node is not adjacent to previous node --> distance to previous node is 2
                    probability = 1D / inOutParam;
                }
                probabilities[index] = probability;
                probSum += probability;
                index++;
                return true;
            }

            private double[] probabilities() {
                return normalizeDistribution(probabilities, probSum);
            }
        }
    }
}
