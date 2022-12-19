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
package org.neo4j.gds.betweenness;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.carrotsearch.hppc.BitSetIterator.NO_MORE;

public interface SelectionStrategy {

    SelectionStrategy ALL = new SelectionStrategy() {
        private final AtomicLong nodeQueue = new AtomicLong();
        private long graphSize;

        @Override
        public void init(Graph graph, ExecutorService executorService, int concurrency) {
            this.graphSize = graph.nodeCount();
            nodeQueue.set(0);
        }

        @Override
        public long next() {
            long next = nodeQueue.getAndIncrement();
            if (next >= graphSize) {
                return -1;
            }
            return next;
        }
    };

    void init(Graph graph, ExecutorService executorService, int concurrency);

    long next();

    class RandomDegree implements SelectionStrategy {

        private final long samplingSize;
        private final Optional<Long> maybeRandomSeed;
        private final AtomicLong nodeQueue = new AtomicLong();

        private long graphSize;
        private BitSet bitSet;

        public RandomDegree(long samplingSize) {
            this(samplingSize, Optional.empty());
        }

        public RandomDegree(long samplingSize, Optional<Long> maybeRandomSeed) {
            this.samplingSize = samplingSize;
            this.maybeRandomSeed = maybeRandomSeed;
        }

        @Override
        public void init(Graph graph, ExecutorService executorService, int concurrency) {
            assert samplingSize <= graph.nodeCount();
            this.bitSet = new BitSet(graph.nodeCount());
            this.graphSize = graph.nodeCount();
            nodeQueue.set(0);
            var partitions = PartitionUtils.numberAlignedPartitioning(concurrency, graph.nodeCount(), Long.SIZE);
            var maxDegree = maxDegree(graph, partitions, executorService, concurrency);
            selectNodes(graph, partitions, maxDegree, executorService, concurrency);
        }

        @Override
        public long next() {
            long next;
            while ((next = nodeQueue.getAndIncrement()) < graphSize) {
                if (bitSet.get(next)) {
                    return next;
                }
            }
            return -1;
        }

        private static int maxDegree(
            Graph graph,
            Collection<Partition> partitions,
            ExecutorService executorService,
            int concurrency
        ) {
            AtomicInteger maxDegree = new AtomicInteger(0);

            var tasks = partitions.stream()
                .map(partition -> (Runnable) () -> partition.consume(nodeId -> {
                    int degree = graph.degree(nodeId);
                    int current = maxDegree.get();
                    while (degree > current) {
                        int newCurrent = maxDegree.compareAndExchange(current, degree);
                        if (newCurrent == current) {
                            break;
                        }
                        current = newCurrent;
                    }
                })).collect(Collectors.toList());

            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(tasks)
                .executor(executorService)
                .run();

            return maxDegree.get();
        }

        private void selectNodes(
            Graph graph,
            Collection<Partition> partitions,
            int maxDegree,
            ExecutorService executorService,
            int concurrency
        ) {
            var random = maybeRandomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
            var selectionSize = new AtomicLong(0);
            var tasks = partitions.stream()
                .map(partition -> (Runnable) () -> {
                    var threadLocalRandom = random.split();
                    var fromNode = partition.startNode();
                    var toNode = partition.startNode() + partition.nodeCount();

                    for (long nodeId = fromNode; nodeId < toNode; nodeId++) {
                        var currentSelectionSize = selectionSize.get();
                        if (currentSelectionSize >= samplingSize) {
                            break;
                        }
                        int nodeDegree = graph.degree(nodeId);
                        // probability factor is in range [1, maxDegree] (inclusive both ends)
                        // the probability of a node being selected is probabilityFactor * (1 / maxDegree)
                        int probabilityFactor = threadLocalRandom.nextInt(maxDegree) + 1;
                        if (probabilityFactor <= nodeDegree) {
                            while (true) {
                                long actualCurrentSelectionSize = selectionSize.compareAndExchange(
                                    currentSelectionSize,
                                    currentSelectionSize + 1
                                );
                                if (currentSelectionSize == actualCurrentSelectionSize) {
                                    bitSet.set(nodeId);
                                    break;
                                }
                                if (actualCurrentSelectionSize >= samplingSize) {
                                    break;
                                }
                                currentSelectionSize = actualCurrentSelectionSize;
                            }
                        }
                    }
                }).collect(Collectors.toList());

            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(tasks)
                .executor(executorService)
                .run();

            long actualSelectedNodes = selectionSize.get();

            if (actualSelectedNodes < samplingSize) {
                // Flip bitset to be able to iterate unset bits.
                // The upper range is Graph#nodeCount() since
                // BitSet#size() returns a multiple of 64.
                // We need to make sure to stay within bounds.
                bitSet.flip(0, graph.nodeCount());
                // Potentially iterate the bitset multiple times
                // until we have exactly numSeedNodes nodes.
                BitSetIterator iterator;
                while (actualSelectedNodes < samplingSize) {
                    iterator = bitSet.iterator();
                    var unselectedNode = iterator.nextSetBit();
                    while (unselectedNode != NO_MORE && actualSelectedNodes < samplingSize) {
                        if (random.nextDouble() >= 0.5) {
                            bitSet.flip(unselectedNode);
                            actualSelectedNodes++;
                        }
                        unselectedNode = iterator.nextSetBit();
                    }
                }
                bitSet.flip(0, graph.nodeCount());
            }
        }
    }
}
