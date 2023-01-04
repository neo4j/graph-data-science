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

import org.immutables.builder.Builder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.core.utils.TerminationFlag.RUN_CHECK_NODE_COUNT;

final class UnsampledStrategy {

    private final Graph graph;
    private final DisjointSetStruct disjointSetStruct;
    private final long batchSize;
    private final int threadSize;

    private final Optional<Double> threshold;

    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private final ExecutorService executorService;

    @Builder.Constructor
    UnsampledStrategy(
        Graph graph,
        DisjointSetStruct disjointSetStruct,
        long batchSize,
        Optional<Double> threshold,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        this.graph = graph;
        this.disjointSetStruct = disjointSetStruct;
        this.batchSize = batchSize;
        this.threadSize = (int) ParallelUtil.threadCount(batchSize, graph.nodeCount());
        this.threshold = threshold;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.executorService = executorService;
    }

    void compute() {
        var tasks = new ArrayList<Runnable>(threadSize);
        for (long offset = 0L; offset < this.graph.nodeCount(); offset += this.batchSize) {
            var unionTask = threshold.isEmpty()
                ? new UnionTask(graph, disjointSetStruct, offset, batchSize, terminationFlag, progressTracker)
                : new UnionWithThresholdTask(
                    graph,
                    disjointSetStruct,
                    threshold.get(),
                    offset,
                    batchSize,
                    terminationFlag,
                    progressTracker
                );

            tasks.add(unionTask);
        }
        ParallelUtil.run(tasks, executorService);
    }

    static class UnionTask implements Runnable, RelationshipConsumer {

        final Graph graph;

        final DisjointSetStruct struct;
        private final long offset;

        private final ProgressTracker progressTracker;
        private final long end;
        private final TerminationFlag terminationFlag;

        UnionTask(
            Graph graph,
            DisjointSetStruct disjointSetStruct,
            long offset,
            long batchSize,
            TerminationFlag terminationFlag,
            ProgressTracker progressTracker
        ) {
            this.graph = graph.concurrentCopy();
            this.struct = disjointSetStruct;
            this.offset = offset;
            this.terminationFlag = terminationFlag;
            this.progressTracker = progressTracker;

            this.end = Math.min(offset + batchSize, graph.nodeCount());
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
            graph.forEachRelationship(node, this);
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId) {
            struct.union(sourceNodeId, targetNodeId);
            return true;
        }
    }

    static class UnionWithThresholdTask extends UnionTask implements RelationshipWithPropertyConsumer {

        private final double threshold;

        UnionWithThresholdTask(
            Graph graph,
            DisjointSetStruct struct,
            double threshold,
            long offset,
            long batchSize,
            TerminationFlag terminationFlag,
            ProgressTracker progressTracker
        ) {
            super(graph, struct, offset, batchSize, terminationFlag, progressTracker);
            this.threshold = threshold;
        }

        @Override
        void compute(final long node) {
            graph.forEachRelationship(node, Wcc.defaultWeight(threshold), this);
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId, final double property) {
            if (property > threshold) {
                struct.union(sourceNodeId, targetNodeId);
            }
            return true;
        }
    }
}
