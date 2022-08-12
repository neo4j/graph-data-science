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
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.TerminationFlag.RUN_CHECK_NODE_COUNT;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Parallel Union-Find Algorithm based on the
 * "Wait-free Parallel Algorithms for the Union-Find Problem" paper.
 *
 * @see HugeAtomicDisjointSetStruct
 * @see <a href="http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.56.8354&rep=rep1&type=pdf">the paper</a>
 *
 * For the undirected case we do subgraph sampling, as introduced in [1].
 *
 * The idea is to identify the largest component using a sampled subgraph.
 * Relationships of nodes that are already contained in the largest component are
 * not iterated. The compression step described in [1], is contained in
 * {@link DisjointSetStruct#setIdOf}.
 *
 * [1] Michael Sutton, Tal Ben-Nun, and Amnon Barak. "Optimizing Parallel
 * Graph Connectivity Computation via Subgraph Sampling" Symposium on
 * Parallel and Distributed Processing, IPDPS 2018.
 */
public class Wcc extends Algorithm<DisjointSetStruct> {

    /**
     * The number of relationships of each node to sample during subgraph sampling.
     */
    private static final int NEIGHBOR_ROUNDS = 2;

    /**
     * The number of samples from the DSS to find the largest component.
     */
    private static final int SAMPLING_SIZE = 1024;

    private final WccBaseConfig config;
    private final NodePropertyValues initialComponents;
    private final ExecutorService executor;
    private final long nodeCount;
    private final long batchSize;
    private final int threadSize;

    private Graph graph;

    public static MemoryEstimation memoryEstimation(boolean incremental) {
        return MemoryEstimations
            .builder(Wcc.class.getSimpleName())
            .add("dss", HugeAtomicDisjointSetStruct.memoryEstimation(incremental))
            .build();
    }

    public Wcc(
        Graph graph,
        ExecutorService executor,
        int minBatchSize,
        WccBaseConfig config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.initialComponents = config.isIncremental()
            ? graph.nodeProperties(config.seedProperty())
            : null;
        this.executor = executor;
        this.nodeCount = graph.nodeCount();
        this.batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            config.concurrency(),
            minBatchSize,
            Integer.MAX_VALUE
        );

        long threadSize = ParallelUtil.threadCount(batchSize, nodeCount);
        if (threadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(formatWithLocale(
                "Too many nodes (%d) to run union find with the given concurrency (%d) and batchSize (%d)",
                nodeCount,
                config.concurrency(),
                batchSize
            ));
        }
        this.threadSize = (int) threadSize;
    }

    @Override
    public DisjointSetStruct compute() {
        progressTracker.beginSubTask();

        long nodeCount = graph.nodeCount();

        DisjointSetStruct dss = config.isIncremental()
            ? new HugeAtomicDisjointSetStruct(nodeCount, initialComponents, config.concurrency())
            : new HugeAtomicDisjointSetStruct(nodeCount, config.concurrency());

        if (graph.schema().isUndirected() && !config.hasThreshold()) {
            computeUndirected(dss);
        } else {
            computeDirected(dss);
        }

        progressTracker.endSubTask();
        return dss;
    }

    @Override
    public void release() {
        graph = null;
    }

    public double threshold() {
        return config.threshold();
    }

    private void computeDirected(DisjointSetStruct dss) {
        var tasks = new ArrayList<Runnable>(threadSize);
        for (long i = 0L; i < this.nodeCount; i += batchSize) {
            var wccTask = !config.hasThreshold()
                ? new DirectedUnionTask(dss, i)
                : new DirectedUnionWithThresholdTask(threshold(), dss, i);
            tasks.add(wccTask);
        }
        ParallelUtil.run(tasks, executor);
    }

    private void computeUndirected(DisjointSetStruct components) {
        var partitions = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            Function.identity(),
            Optional.empty()
        );

        sampleSubgraph(components, partitions);
        long largestComponent = findLargestComponent(components);
        linkRemaining(components, partitions, largestComponent);
    }

    /**
     * Processes a sparse samples subgraph first for approximating components.
     * Samples by processing a fixed number of neighbors for each node.
     */
    private void sampleSubgraph(DisjointSetStruct components, List<Partition> partitions) {
        var tasks = partitions
            .stream()
            .map(partition -> new UndirectedSamplingTask(
                graph,
                partition,
                components,
                progressTracker,
                terminationFlag
            ))
            .collect(Collectors.toList());

        ParallelUtil.run(tasks, executor);
    }

    /**
     * Approximates the largest component by sampling a fixed number of nodes.
     */
    private long findLargestComponent(DisjointSetStruct components) {
        var random = new SplittableRandom();
        var sampleCounts = new LongIntHashMap();

        for (int i = 0; i < SAMPLING_SIZE; i++) {
            var node = random.nextLong(nodeCount);
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
            .map(partition -> new UndirectedUnionTask(
                graph,
                partition,
                largestComponent,
                components,
                progressTracker,
                terminationFlag
            ))
            .collect(Collectors.toList());
        ParallelUtil.run(tasks, executor);
    }

    private static double defaultWeight(double threshold) {
        return threshold + 1;
    }

    private class DirectedUnionTask implements Runnable, RelationshipConsumer {

        final DisjointSetStruct struct;
        final RelationshipIterator rels;
        private final long offset;
        private final long end;

        DirectedUnionTask(DisjointSetStruct struct, long offset) {
            this.struct = struct;
            this.rels = graph.concurrentCopy();
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        public void run() {
            for (long node = offset; node < end; node++) {
                compute(node);
                if (node % RUN_CHECK_NODE_COUNT == 0) {
                    terminationFlag.assertRunning();
                }

                progressTracker.logProgress(graph.degree(node));
            }
        }

        void compute(final long node) {
            rels.forEachRelationship(node, this);
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId) {
            struct.union(sourceNodeId, targetNodeId);
            return true;
        }
    }

    private class DirectedUnionWithThresholdTask extends DirectedUnionTask implements RelationshipWithPropertyConsumer {

        private final double threshold;

        DirectedUnionWithThresholdTask(double threshold, DisjointSetStruct struct, long offset) {
            super(struct, offset);
            this.threshold = threshold;
        }

        @Override
        void compute(final long node) {
            rels.forEachRelationship(node, Wcc.defaultWeight(threshold), this);
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId, final double property) {
            if (property > threshold) {
                struct.union(sourceNodeId, targetNodeId);
            }
            return true;
        }
    }

    static final class UndirectedSamplingTask implements Runnable, RelationshipConsumer {

        private final Graph graph;
        private final Partition partition;
        private final DisjointSetStruct components;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;
        private long limit;

        UndirectedSamplingTask(
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

        public void reset() {
            limit = NEIGHBOR_ROUNDS;
        }

    }

    static final class UndirectedUnionTask implements Runnable, RelationshipConsumer {

        private final Graph graph;
        private final long skipComponent;
        private final Partition partition;
        private final DisjointSetStruct components;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;
        private long skip;

        UndirectedUnionTask(
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

        public void reset() {
            skip = 0;
        }

    }
}
