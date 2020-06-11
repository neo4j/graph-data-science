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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public abstract class SelectionStrategy {

    abstract void init(Graph graph, ExecutorService executorService, int concurrency);

    abstract boolean select(long nodeId);

    abstract long size();

    public enum Strategy {
        ALL {
            public SelectionStrategy create(double probability) {
                return new All();
            }
        },
        RANDOM {
            public SelectionStrategy create(double probability) {
                return new Random(probability);
            }
        },
        RANDOM_DEGREE {
            public SelectionStrategy create(double probability) {
                return new RandomDegree(probability);
            }
        };

        public abstract SelectionStrategy create(double probability);

        public static Strategy of(String value) {
            try {
                return Strategy.valueOf(value.toUpperCase(Locale.ENGLISH));
            } catch (IllegalArgumentException e) {
                String availableProjections = Arrays
                    .stream(Strategy.values())
                    .map(Strategy::name)
                    .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(formatWithLocale(
                    "Selection strategy `%s` is not supported. Must be one of: %s.",
                    value,
                    availableProjections
                ));
            }
        }

        public static Strategy parse(Object object) {
            if (object == null) {
                return null;
            }
            if (object instanceof String) {
                return of(((String) object).toUpperCase(Locale.ENGLISH));
            }
            if (object instanceof Strategy) {
                return (Strategy) object;
            }
            return null;
        }
    }

    private static class All extends SelectionStrategy {

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

    private static class Random extends SelectionStrategy {

        private final double probability;
        private BitSet bitSet;
        private long size;

        Random(double probability) {
            this.probability = probability;
        }

        @Override
        void init(Graph graph, ExecutorService executorService, int concurrency) {
            this.bitSet = new BitSet(graph.nodeCount());
            selectNodes(graph, probability, executorService, concurrency);
            this.size = this.bitSet.cardinality();
        }

        @Override
        boolean select(long nodeId) {
            return bitSet.get(nodeId);
        }

        @Override
        long size() {
            return size;
        }

        private void selectNodes(Graph graph, double probability, ExecutorService executorService, int concurrency) {
            var random = new SecureRandom();
            var tasks = PartitionUtils.numberAlignedPartitioning(concurrency, graph.nodeCount(), Long.SIZE)
                .stream()
                .map(partition -> (Runnable) () -> {
                    for (long nodeId = partition.startNode; nodeId < partition.startNode + partition.nodeCount; nodeId++) {
                        if (random.nextDouble() < probability) {
                            this.bitSet.set(nodeId);
                        }
                    }
                }).collect(Collectors.toList());

            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);
        }
    }

    private static class RandomDegree extends SelectionStrategy {

        private final double probability;
        private BitSet bitSet;
        private long size;

        RandomDegree(double probability) {
            this.probability = probability;
        }

        @Override
        void init(Graph graph, ExecutorService executorService, int concurrency) {
            this.bitSet = new BitSet(graph.nodeCount());
            var partitions = PartitionUtils.numberAlignedPartitioning(concurrency, graph.nodeCount(), Long.SIZE);
            var maxDegree = maxDegree(graph, partitions, executorService, concurrency);
            selectNodes(graph, partitions, probability, maxDegree, executorService, concurrency);
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
            AtomicInteger mx = new AtomicInteger(0);

            var tasks = partitions.stream()
                .map(partition -> (Runnable) () -> {
                    for (long nodeId = partition.startNode; nodeId < partition.startNode + partition.nodeCount; nodeId++) {
                        int degree = graph.degree(nodeId);
                        int current;
                        do {
                            current = mx.get();
                        } while (degree > current && !mx.compareAndSet(current, degree));
                    }
                }).collect(Collectors.toList());

            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

            return mx.get();
        }

        private void selectNodes(
            Graph graph,
            Collection<Partition> partitions,
            double probabilityOffset,
            double maxDegree,
            ExecutorService executorService,
            int concurrency
        ) {
            var random = new SecureRandom();
            var tasks = partitions.stream()
                .map(partition -> (Runnable) () -> {
                    for (long nodeId = partition.startNode; nodeId < partition.startNode + partition.nodeCount; nodeId++) {
                        if (random.nextDouble() - probabilityOffset <= graph.degree(nodeId) / maxDegree) {
                            bitSet.set(nodeId);
                        }
                    }
                }).collect(Collectors.toList());

            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);
        }
    }
}
