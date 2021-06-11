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
package org.neo4j.graphalgo.wcc;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.procedures.LongIntProcedure;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.partition.Partition;

import java.util.Collection;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * This CC implementation makes use of the Afforest subgraph sampling algorithm [1],
 * which restructures and extends the Shiloach-Vishkin algorithm [2].
 *
 * [1] Michael Sutton, Tal Ben-Nun, and Amnon Barak. "Optimizing Parallel
 * Graph Connectivity Computation via Subgraph Sampling" Symposium on
 * Parallel and Distributed Processing, IPDPS 2018.
 * [2] Yossi Shiloach and Uzi Vishkin. "An o(logn) parallel connectivity algorithm"
 * Journal of Algorithms, 3(1):57–67, 1982.
 */
final class Afforest {

    private static final int NEIGHBOR_ROUNDS = 2;

    private static final int SAMPLING_SIZE = 1024;

    static void sampleSubgraph(
        Graph graph,
        DisjointSetStruct components,
        Collection<Partition> partitions,
        ExecutorService executor
    ) {
        var tasks = partitions
            .stream()
            .map(partition -> new Afforest.SampleSubgraphTask(graph, partition, components))
            .collect(Collectors.toList());

        // Process a sparse sampled subgraph first for approximating components.
        // Sample by processing a fixed number of neighbors for each node (see paper)
        for (int r = 0; r < NEIGHBOR_ROUNDS; r++) {
            for (var task : tasks) {
                task.setTargetIndex(r);
            }
            ParallelUtil.run(tasks, executor);
        }
    }

    static long sampleFrequentElement(long nodeCount, DisjointSetStruct components) {
        var random = new SplittableRandom();
        var sampleCounts = new LongIntHashMap();

        // Sample elements from 'comp'
        for (int i = 0; i < SAMPLING_SIZE; i++) {
            var node = random.nextLong(nodeCount);
            sampleCounts.addTo(components.setIdOf(node), 1);
        }
        // Find most frequent element in samples (estimate of most frequent overall)
        var max = new MutableInt(-1);
        var mostFrequent = new MutableLong(-1L);

        sampleCounts.forEach((LongIntProcedure) (component, count) -> {
            if (count > max.intValue()) {
                max.setValue(count);
                mostFrequent.setValue(component);
            }
        });

        return mostFrequent.longValue();
    }

    static void linkRemaining(
        Graph graph,
        DisjointSetStruct components,
        long largestComponent,
        Collection<Partition> partitions,
        ExecutorService executor
    ) {
        var tasks = partitions
            .stream()
            .map(partition -> new LinkTask(graph, partition, largestComponent, components))
            .collect(Collectors.toList());
        ParallelUtil.run(tasks, executor);
    }

    static final class SampleSubgraphTask implements Runnable {

        private final Graph graph;
        private final Partition partition;
        private final DisjointSetStruct components;

        private int targetIndex;

        SampleSubgraphTask(Graph graph, Partition partition, DisjointSetStruct components) {
            this.graph = graph.concurrentCopy();
            this.partition = partition;
            this.components = components;
        }

        void setTargetIndex(int targetIndex) {
            this.targetIndex = targetIndex;
        }

        @Override
        public void run() {
            var startNode = partition.startNode();
            var endNode = startNode + partition.nodeCount();
            var consumer = new GetTargetConsumer(0);

            for (long node = startNode; node < endNode; node++) {
                if (graph.degree(node) > targetIndex) {
                    consumer.count = targetIndex;
                    graph.forEachRelationship(node, consumer);
                    components.union(node, consumer.target);
                }
            }
        }
    }

    static final class LinkTask implements Runnable {

        private final Graph graph;
        private final long skipComponent;
        private final Partition partition;
        private final DisjointSetStruct components;

        LinkTask(
            Graph graph,
            Partition partition,
            long skipComponent,
            DisjointSetStruct components
        ) {
            this.graph = graph.concurrentCopy();
            this.skipComponent = skipComponent;
            this.partition = partition;
            this.components = components;
        }

        @Override
        public void run() {
            var startNode = partition.startNode();
            var endNode = startNode + partition.nodeCount();

            var linkConsumer = new LinkConsumer(0, components);
            for (long node = startNode; node < endNode; node++) {
                if (components.setIdOf(node) == skipComponent) {
                    continue;
                }
                if (graph.degree(node) > NEIGHBOR_ROUNDS) {
                    // link remaining relationships
                    linkConsumer.skip = NEIGHBOR_ROUNDS;
                    graph.forEachRelationship(node, linkConsumer);
                }
            }
        }
    }

    static class GetTargetConsumer implements RelationshipConsumer {
        long count;
        long target;

        GetTargetConsumer(long count) {
            this.count = count;
        }

        @Override
        public boolean accept(long s, long t) {
            if (count-- == 0) {
                target = t;
                return false;
            }
            return true;
        }
    }

    static class LinkConsumer implements RelationshipConsumer {
        final DisjointSetStruct components;
        long skip;

        LinkConsumer(long skip, DisjointSetStruct components) {
            this.skip = skip;
            this.components = components;
        }

        @Override
        public boolean accept(long source, long target) {
            if (skip-- <= 0) {
                components.union(source, target);
            }
            return true;
        }
    }

    private Afforest() {}
}
