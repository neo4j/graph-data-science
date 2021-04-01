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
package org.neo4j.gds.embeddings.node2vec;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.graphalgo.core.utils.queue.QueueBasedSpliterator;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RandomWalk extends Algorithm<RandomWalk, Stream<long[]>> {

    private final Graph graph;
    private final int steps;
    private final int concurrency;
    private final int walksPerNode;
    private final int queueSize;
    private final double returnParam;
    private final double inOutParam;

    public RandomWalk(
        Graph graph,
        int steps,
        int concurrency,
        int walksPerNode,
        int queueSize,
        double returnParam,
        double inOutParam
    ) {
        this.graph = graph;
        this.steps = steps;
        this.concurrency = concurrency;
        this.walksPerNode = walksPerNode;
        this.queueSize = queueSize;
        this.returnParam = returnParam;
        this.inOutParam = inOutParam;
    }

    @Override
    public Stream<long[]> compute() {
        int timeout = 100;
        BlockingQueue<long[]> walks = new ArrayBlockingQueue<>(queueSize);
        long[] TOMB = new long[0];

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            BitUtil.ceilDiv(graph.nodeCount(), concurrency),
            (partition) -> new RandomWalkTask(
                partition,
                graph.concurrentCopy(),
                walksPerNode,
                steps,
                returnParam,
                inOutParam,
                walks
            )
        );

        new Thread(() -> {
            ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            try {
                walks.put(TOMB);
            } catch (InterruptedException e) {
            }
        }).start();
        QueueBasedSpliterator<long[]> spliterator = new QueueBasedSpliterator<>(walks, TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public RandomWalk me() {
        return this;
    }

    @Override
    public void release() { }

    private static class RandomWalkTask implements Runnable {
        private final Partition partition;
        private final Graph graph;
        private final int numWalks;
        private final int walkLength;
        private final double returnParam;
        private final double inOutParam;
        private final Random random;
        private final BlockingQueue<long[]> walks;
        private MutableInt index;
        private MutableLong randomNeighbour;
        private final long[][] buffer;
        private MutableInt bufferPosition;

        public RandomWalkTask(
            Partition partition,
            Graph graph,
            int numWalks,
            int walkLength,
            double returnParam,
            double inOutParam,
            BlockingQueue<long[]> walks
        ) {
            this.partition = partition;
            this.graph = graph;
            this.numWalks = numWalks;
            this.walkLength = walkLength;
            this.returnParam = returnParam;
            this.inOutParam = inOutParam;
            this.walks = walks;

            this.random = new Random();
            this.index = new MutableInt(0);
            this.randomNeighbour = new MutableLong(-1);
            this.buffer = new long[1000][];
            this.bufferPosition = new MutableInt(0);
        }

        @Override
        public void run() {
            var maxProbability = Math.max(Math.max(1 / returnParam, 1.0), 1 / inOutParam);

            var normalizedReturnProbability = (1 / returnParam) / maxProbability;
            var normalizedSameDistanceProbability = 1 / maxProbability;
            var normalizedInOutProbability = (1 / inOutParam) / maxProbability;

            partition.consume(nodeId -> {
                if (graph.degree(nodeId) <= 0) {
                    return;
                }

                for (int walkIndex = 0; walkIndex < numWalks; walkIndex++) {
                    var walk = new long[walkLength];
                    Arrays.fill(walk, -1L);
                    walk[0] = nodeId;
                    walk[1] = randomNeighbour(nodeId);

                    for (int i = 2; i < walkLength; i++) {
                        var currentNode = walk[i - 1];
                        var currentNodeDegree = graph.degree(currentNode);

                        if (currentNodeDegree == 0) {
                            // We have arrived at a node with no outgoing neighbors, we can stop walking
                            break;
                        } else if (currentNodeDegree == 1) {
                            // This node only has one neighbour, no need to test
                            walk[i] = randomNeighbour(currentNode);
                        } else {
                            long newNode;
                            while (true) {
                                newNode = randomNeighbour(currentNode);
                                var r = random.nextDouble();
                                if (newNode == walk[i -2]) {
                                    if (r < normalizedReturnProbability) {
                                        break;
                                    }
                                } else if (isNeighbour(walk[i -2], newNode)) {
                                    if (r < normalizedSameDistanceProbability) {
                                        break;
                                    }
                                } else if ( r < normalizedInOutProbability) {
                                    break;
                                }
                            }
                            walk[i] = newNode;
                        }
                    }

                    buffer[bufferPosition.getAndIncrement()] = walk;
                    if (bufferPosition.getValue() == buffer.length) {
                        flushBuffer();
                    }
                }
            });

            flushBuffer();
            System.out.println("Thread finished at = " + System.currentTimeMillis());
        }

        private long randomNeighbour(long node) {
            var degree= graph.degree(node);

            var randomNeighbourIndex = random.nextInt(degree);

            index.setValue(0);
            randomNeighbour.setValue(-1);

            graph.forEachRelationship(node, (source, target) -> {
                if (randomNeighbourIndex == index.getValue()) {
                    randomNeighbour.setValue(target);
                    return false;
                }
                index.increment();
                return true;
            });

            return randomNeighbour.getValue();
        }

        private boolean isNeighbour(long source, long target) {
            return graph.exists(source, target);
        }

        private void flushBuffer() {
            for (int i = 0; i < bufferPosition.getValue(); i++) {
                try {
                    walks.put(buffer[i]);
                } catch (InterruptedException e) {

                }
            }
            bufferPosition.setValue(0);
        }
    }
}
