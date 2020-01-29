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
package org.neo4j.graphalgo.impl.walking;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntBinaryPredicate;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.PrimitiveIterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

public class RandomWalk extends Algorithm<RandomWalk, Stream<long[]>> {

    private final Graph graph;
    private final int steps;
    private final NextNodeStrategy strategy;
    private final int concurrency;
    private final int limit;
    private final PrimitiveIterator.OfInt idStream;

    public RandomWalk(
        Graph graph,
        int steps,
        NextNodeStrategy strategy,
        int concurrency,
        int limit,
        PrimitiveIterator.OfInt idStream
    ) {
        this.graph = graph;
        this.steps = steps;
        this.strategy = strategy;
        this.concurrency = concurrency;
        this.limit = limit;
        this.idStream = idStream;
    }

    @Override
    public Stream<long[]> compute() {
        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustedBatchSize(limit, concurrency, 100);
        Collection<Runnable> tasks = new ArrayList<>((limit / batchSize) + 1);

        ArrayBlockingQueue<long[]> queue = new ArrayBlockingQueue<>(queueSize);
        long[] TOMB = new long[0];

        while (idStream.hasNext()) {
            int[] ids = new int[batchSize];
            int i = 0;
            while (i < batchSize && idStream.hasNext()) {
                ids[i++] = idStream.nextInt();
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

    private long[] doWalk(int startNodeId) {
        long[] nodeIds = new long[steps + 1];
        int currentNodeId = startNodeId;
        int previousNodeId = currentNodeId;
        nodeIds[0] = toOriginalNodeId(currentNodeId);
        for (int i = 1; i <= steps; i++) {
            int nextNodeId = Math.toIntExact(strategy.getNextNode(currentNodeId, previousNodeId));
            previousNodeId = currentNodeId;
            currentNodeId = nextNodeId;

            if (currentNodeId == -1 || !running()) {
                // End walk when there is no way out and return empty result
                return Arrays.copyOf(nodeIds, 1);
            }
            nodeIds[i] = toOriginalNodeId(currentNodeId);
        }

        return nodeIds;
    }

    private long toOriginalNodeId(int currentNodeId) {
        return currentNodeId == -1 ? -1 : graph.toOriginalNodeId(currentNodeId);
    }

    private static <T> void put(BlockingQueue<T> queue, T items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public abstract static class NextNodeStrategy {
        protected Graph graph;
        protected Degrees degrees;

        public NextNodeStrategy(Graph graph, Degrees degrees) {
            this.graph = graph;
            this.degrees = degrees;
        }

        public abstract long getNextNode(long currentNodeId, long previousNodeId);
    }

    public static class RandomNextNodeStrategy extends NextNodeStrategy {

        public RandomNextNodeStrategy(Graph graph, Degrees degrees) {
            super(graph, degrees);
        }

        @Override
        public long getNextNode(long currentNodeId, long previousNodeId) {
            int degree = degrees.degree(currentNodeId);
            if (degree == 0) {
                return -1;
            }
            int randomEdgeIndex = ThreadLocalRandom.current().nextInt(degree);

            MutableLong targetNodeId = new MutableLong(-1L);
            MutableInt counter = new MutableInt(0);
            graph.concurrentCopy().forEachRelationship(currentNodeId, (s, t) -> {
                if (counter.getAndIncrement() == randomEdgeIndex) {
                    targetNodeId.setValue(t);
                    return false;
                }
                return true;
            });

            return targetNodeId.getValue();
        }

    }

    public static class Node2VecStrategy extends NextNodeStrategy {
        private double returnParam, inOutParam;


        public Node2VecStrategy(Graph graph, Degrees degrees, double returnParam, double inOutParam) {
            super(graph, degrees);
            this.returnParam = returnParam;
            this.inOutParam = inOutParam;
        }

        public long getNextNode(long currentNode, long previousNode) {
            int currentNodeId = Math.toIntExact(currentNode);
            int previousNodeId = Math.toIntExact(previousNode);

            int degree = degrees.degree(currentNodeId);
            if (degree == 0) {
                return -1;
            }

            double[] distribution = buildProbabilityDistribution(currentNodeId, previousNodeId, returnParam, inOutParam, degree);
            int neighbourIndex = pickIndexFromDistribution(distribution, ThreadLocalRandom.current().nextDouble());

            return graph.getTarget(currentNodeId, neighbourIndex);
        }

        private double[] buildProbabilityDistribution(int currentNodeId, int previousNodeId,
                                                      double returnParam, double inOutParam, int degree) {
            ProbabilityDistributionComputer consumer = new ProbabilityDistributionComputer(degree, currentNodeId, previousNodeId, returnParam, inOutParam);
            graph.concurrentCopy().forEachRelationship(currentNodeId, longToIntConsumer(consumer));
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

        private class ProbabilityDistributionComputer implements IntBinaryPredicate {
            final double[] probabilities;
            private final int currentNodeId;
            private final int previousNodeId;
            private final double returnParam;
            private final double inOutParam;
            double probSum;
            int index;

            public ProbabilityDistributionComputer(int degree, int currentNodeId, int previousNodeId, double returnParam, double inOutParam) {
                this.currentNodeId = currentNodeId;
                this.previousNodeId = previousNodeId;
                this.returnParam = returnParam;
                this.inOutParam = inOutParam;
                probabilities = new double[degree];
                probSum = 0;
                index = 0;
            }

            @Override
            public boolean test(int start, int end) {
                int neighbourId = start == currentNodeId ? end : start;

                double probability;

                if (neighbourId == previousNodeId) {
                    // node is previous node
                    probability = 1D / returnParam;
                } else if (graph.exists(previousNodeId, neighbourId)) {
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
