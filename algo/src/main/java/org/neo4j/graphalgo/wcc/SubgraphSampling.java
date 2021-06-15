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
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.partition.Partition;

import javax.management.relation.Relation;
import java.util.Collection;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * The subgraph sampling optimization has been introduced in [1].
 *
 * The idea is to identify the largest component using a sampled subgraph.
 * Relationships of nodes that are already contained in the largest component are
 * not iterated.
 *
 * This approach requires the relationships to be undirected to make sure that
 * skipped nodes are eventually assigned to the correct component in case the
 * sampled one was not representing the largest component.
 *
 * In contrast to [1], this implementation uses a {@link DisjointSetStruct} to
 * represent the mapping between nodes and components. The compression step
 * described in [1], is contained in {@link DisjointSetStruct#setIdOf}.
 *
 * [1] Michael Sutton, Tal Ben-Nun, and Amnon Barak. "Optimizing Parallel
 * Graph Connectivity Computation via Subgraph Sampling" Symposium on
 * Parallel and Distributed Processing, IPDPS 2018.
 */
final class SubgraphSampling {

    /**
     * The number of relationships of each node that
     * we look at during the initial sampling round.
     */
    private static final int NEIGHBOR_ROUNDS = 2;

    /**
     * The number of samples we draw from the node
     * space to identify the largest component.
     */
    private static final int SAMPLING_SIZE = 1024;

    /**
     * Processes a sparse samples subgraph first for approximating components.
     * Samples by processing a fixed number of neighbors for each node.
     */
    static void sampleSubgraph(
        Graph graph,
        DisjointSetStruct components,
        Collection<Partition> partitions,
        ExecutorService executor
    ) {
        var tasks = partitions
            .stream()
            .map(partition -> new SubgraphSampling.SampleSubgraphTask(graph, partition, components))
            .collect(Collectors.toList());

        ParallelUtil.run(tasks, executor);
    }

    /**
     * Finds the most frequent component by sampling a fixed number of nodes.
     */
    static long sampleFrequentElement(DisjointSetStruct components, long nodeCount) {
        var random = new SplittableRandom();
        var sampleCounts = new LongIntHashMap();

        for (int i = 0; i < SAMPLING_SIZE; i++) {
            var node = random.nextLong(nodeCount);
            sampleCounts.addTo(components.setIdOf(node), 1);
        }

        var max = new MutableInt(-1);
        var mostFrequent = new MutableLong(-1L);
        for (LongIntCursor entry : sampleCounts) {
            var component = entry.key;
            var count = entry.value;

            if (count > max.intValue()) {
                max.setValue(count);
                mostFrequent.setValue(component);
            }
        }

        return mostFrequent.longValue();
    }

    /**
     * Processes the remaining relationships that were
     * not processed during the initial sampling.
     *
     * Skips nodes that are already contained in the largest component.
     */
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

    static final class SampleSubgraphTask implements Runnable, RelationshipConsumer {

        private final Graph graph;
        private final Partition partition;
        private final DisjointSetStruct components;
        private long limit;

        SampleSubgraphTask(Graph graph, Partition partition, DisjointSetStruct components) {
            this.graph = graph.concurrentCopy();
            this.partition = partition;
            this.components = components;
        }

        @Override
        public void run() {
            var startNode = partition.startNode();
            var endNode = startNode + partition.nodeCount();

            for (long node = startNode; node < endNode; node++) {
                reset();
                graph.forEachRelationship(node, this);
            }
        }

        @Override
        public boolean accept(long s, long t) {
            components.union(s, t);
            limit--;
            return limit != 0;
        }

        public void reset() {
            limit = NEIGHBOR_ROUNDS;
        }

    }

    static final class LinkTask implements Runnable, RelationshipConsumer {

        private final Graph graph;
        private final long skipComponent;
        private final Partition partition;
        private final DisjointSetStruct components;
        private long skip;

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

            for (long node = startNode; node < endNode; node++) {
                if (components.setIdOf(node) == skipComponent) {
                    continue;
                }
                if (graph.degree(node) > NEIGHBOR_ROUNDS) {
                    reset();
                    graph.forEachRelationship(node, this);
                }
            }
        }

        @Override
        public boolean accept(long source, long target) {
            skip++;
            if (skip > NEIGHBOR_ROUNDS) {
                components.union(source, target);
            }
            return true;
        }

        public void reset() {
            skip = 0;
        }

    }

    private SubgraphSampling() {}
}
