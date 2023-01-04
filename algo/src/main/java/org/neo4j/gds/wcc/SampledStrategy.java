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
package org.neo4j.gds.wcc;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.immutables.builder.Builder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.TerminationFlag.RUN_CHECK_NODE_COUNT;

final class SampledStrategy {

    /**
     * The number of relationships of each node to sample during subgraph sampling.
     */
    private static final int NEIGHBOR_ROUNDS = 2;
    /**
     * The number of samples from the DSS to find the largest component.
     */
    private static final int SAMPLING_SIZE = 1024;

    private final Graph graph;
    private final DisjointSetStruct disjointSetStruct;
    private final int concurrency;

    private final Optional<Double> threshold;

    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private final ExecutorService executorService;

    @Builder.Constructor
    SampledStrategy(
        Graph graph,
        DisjointSetStruct disjointSetStruct,
        int concurrency,
        Optional<Double> threshold,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        this.graph = graph;
        this.disjointSetStruct = disjointSetStruct;
        this.concurrency = concurrency;
        this.threshold = threshold;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.executorService = executorService;
    }

    void compute() {
        var partitions = PartitionUtils.rangePartition(
            this.concurrency,
            graph.nodeCount(),
            Function.identity(),
            Optional.empty()
        );

        sampleSubgraph(disjointSetStruct, partitions);
        long largestComponent = findLargestComponent(disjointSetStruct);
        linkRemaining(disjointSetStruct, partitions, largestComponent);
    }

    /**
     * Processes a sparse samples subgraph first for approximating components.
     * Samples by processing a fixed number of neighbors for each node.
     */
    private void sampleSubgraph(DisjointSetStruct components, List<Partition> partitions) {
        var tasks = partitions
            .stream()
            .map(partition -> new SampledStrategy.SamplingTask(
                graph,
                partition,
                components,
                progressTracker,
                terminationFlag
            ))
            .collect(Collectors.toList());

        ParallelUtil.run(tasks, executorService);
    }

    /**
     * Approximates the largest component by sampling a fixed number of nodes.
     */
    private long findLargestComponent(DisjointSetStruct components) {
        var random = new SplittableRandom();
        var sampleCounts = new LongIntHashMap();

        for (int i = 0; i < SAMPLING_SIZE; i++) {
            var node = random.nextLong(graph.nodeCount());
            sampleCounts.addTo(components.setIdOf(node), 1);
        }

        var max = -1;
        var mostFrequent = -1L;
        for (LongIntCursor entry : sampleCounts) {
            var component = entry.key;
            var count = entry.value;

            if (count > max) {
                max = count;
                mostFrequent = component;
            }
        }

        return mostFrequent;
    }

    /**
     * Processes the remaining relationships that were not processed during the initial sampling.
     *
     * Skips nodes that are already contained in the largest component.
     */
    private void linkRemaining(DisjointSetStruct components, List<Partition> partitions, long largestComponent) {
        var tasks = partitions
            .stream()
            .map(partition -> new SampledStrategy.LinkTask(
                graph,
                partition,
                largestComponent,
                components,
                progressTracker,
                terminationFlag
            ))
            .collect(Collectors.toList());
        ParallelUtil.run(tasks, executorService);
    }

    static final class SamplingTask implements Runnable, RelationshipConsumer {

        private final Graph graph;

        private final Partition partition;
        private final DisjointSetStruct components;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;
        private long limit;

        SamplingTask(
            Graph graph,
            Partition partition,
            DisjointSetStruct components,
            ProgressTracker progressTracker,
            TerminationFlag terminationFlag
        ) {
            this.graph = graph.concurrentCopy();
            this.partition = partition;
            this.components = components;
            this.progressTracker = progressTracker;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public void run() {
            var startNode = partition.startNode();
            var endNode = startNode + partition.nodeCount();

            for (long node = startNode; node < endNode; node++) {
                reset();
                graph.forEachRelationship(node, this);

                if (node % RUN_CHECK_NODE_COUNT == 0) {
                    terminationFlag.assertRunning();
                }
                progressTracker.logProgress(Math.min(NEIGHBOR_ROUNDS, graph.degree(node)));
            }
        }

        @Override
        public boolean accept(long s, long t) {
            components.union(s, t);
            limit--;
            return limit != 0;
        }

        void reset() {
            limit = NEIGHBOR_ROUNDS;
        }
    }

    static final class LinkTask implements Runnable, RelationshipConsumer {

        private final Graph graph;

        private final long skipComponent;
        private final Partition partition;
        private final DisjointSetStruct components;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;
        private long skip;

        LinkTask(
            Graph graph,
            Partition partition,
            long skipComponent,
            DisjointSetStruct components,
            ProgressTracker progressTracker,
            TerminationFlag terminationFlag
        ) {
            this.graph = graph.concurrentCopy();
            this.skipComponent = skipComponent;
            this.partition = partition;
            this.components = components;
            this.progressTracker = progressTracker;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public void run() {
            var startNode = partition.startNode();
            var endNode = startNode + partition.nodeCount();

            for (long node = startNode; node < endNode; node++) {
                if (components.setIdOf(node) == skipComponent) {
                    continue;
                }
                var degree = graph.degree(node);
                if (degree > NEIGHBOR_ROUNDS) {
                    reset();
                    graph.forEachRelationship(node, this);

                    progressTracker.logProgress(degree - NEIGHBOR_ROUNDS);
                    if (node % RUN_CHECK_NODE_COUNT == 0) {
                        terminationFlag.assertRunning();
                    }
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

        void reset() {
            skip = 0;
        }
    }
}
