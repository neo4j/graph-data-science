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
package org.neo4j.graphalgo.betweenness;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.carrotsearch.hppc.BitSetIterator.NO_MORE;

public abstract class SelectionStrategy {

    abstract void init(Graph graph, ExecutorService executorService, int concurrency);

    abstract boolean select(long nodeId);

    abstract long size();

    public static class All extends SelectionStrategy {

        private long nodeCount;

        @Override
        void init(Graph graph, ExecutorService executorService, int concurrency) {
            this.nodeCount = graph.nodeCount();
        }

        @Override
        boolean select(long nodeId) {
            return true;
        }

        @Override
        long size() {
            return nodeCount;
        }
    }

    public static class RandomDegree extends SelectionStrategy {

        private final long numSeedNodes;
        private final Optional<Long> maybeRandomSeed;

        private BitSet bitSet;
        private long size;

        public RandomDegree(long numSeedNodes) {
            this(numSeedNodes, Optional.empty());
        }

        public RandomDegree(long numSeedNodes, Optional<Long> maybeRandomSeed) {
            this.numSeedNodes = numSeedNodes;
            this.maybeRandomSeed = maybeRandomSeed;
        }

        @Override
        void init(Graph graph, ExecutorService executorService, int concurrency) {
            assert numSeedNodes <= graph.nodeCount();
            this.bitSet = new BitSet(graph.nodeCount());
            var partitions = PartitionUtils.numberAlignedPartitioning(concurrency, graph.nodeCount(), Long.SIZE);
            var maxDegree = maxDegree(graph, partitions, executorService, concurrency);
            selectNodes(graph, partitions, maxDegree, executorService, concurrency);
            this.size = bitSet.cardinality();
        }

        @Override
        boolean select(long nodeId) {
            return bitSet.get(nodeId);
        }

        @Override
        long size() {
            return size;
        }

        private long maxDegree(
            Graph graph,
            Collection<Partition> partitions,
            ExecutorService executorService,
            int concurrency
        ) {
            AtomicInteger maxDegree = new AtomicInteger(0);

            var tasks = partitions.stream()
                .map(partition -> (Runnable) () -> {
                    var fromNode = partition.startNode;
                    var toNode = partition.startNode + partition.nodeCount;

                    for (long nodeId = fromNode; nodeId < toNode; nodeId++) {
                        int degree = graph.degree(nodeId);
                        int current = maxDegree.get();
                        while (degree > current) {
                            int newCurrent = maxDegree.compareAndExchange(current, degree);
                            if (newCurrent == current) {
                                break;
                            }
                            current = newCurrent;
                        }
                    }
                }).collect(Collectors.toList());

            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

            return maxDegree.get();
        }

        private void selectNodes(
            Graph graph,
            Collection<Partition> partitions,
            double maxDegree,
            ExecutorService executorService,
            int concurrency
        ) {
            var random = maybeRandomSeed.map(Random::new).orElseGet(Random::new);
            var numSelectNodes = new AtomicLong(0);
            var tasks = partitions.stream()
                .map(partition -> (Runnable) () -> {
                    var fromNode = partition.startNode;
                    var toNode = partition.startNode + partition.nodeCount;

                    for (long nodeId = fromNode; nodeId < toNode && numSelectNodes.get() < numSeedNodes; nodeId++) {
                        if (random.nextDouble() <= graph.degree(nodeId) / maxDegree) {
                            bitSet.set(nodeId); // (0, 100]
                            numSelectNodes.getAndIncrement();
                        }
                    }
                }).collect(Collectors.toList());

            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

            long actualSelectedNodes = numSelectNodes.get();

            if (actualSelectedNodes < numSeedNodes) {
                // Flip bitset to be able to iterate unset bits
                bitSet.flip(0, bitSet.size());
                // BitSet#size() returns a multiple of 64.
                // We need to make sure to stay within bounds.
                bitSet.clear(graph.nodeCount(), bitSet.size());
                // Potentially iterate the bitset multiple times
                // until we have exactly numSeedNodes nodes.
                BitSetIterator iterator;
                while (actualSelectedNodes < numSeedNodes) {
                    iterator = bitSet.iterator();
                    var unselectedNode = iterator.nextSetBit();
                    while (unselectedNode != NO_MORE && actualSelectedNodes < numSeedNodes) {
                        if (random.nextDouble() >= 0.5) {
                            bitSet.flip(unselectedNode);
                            actualSelectedNodes++;
                        }
                        unselectedNode = iterator.nextSetBit();
                    }
                }
                bitSet.flip(0, bitSet.size());
            }
        }
    }
}
