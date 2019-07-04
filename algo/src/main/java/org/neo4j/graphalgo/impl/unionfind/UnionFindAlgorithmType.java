/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.unionfind;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;

/**
 * this class is basically a helper to instantiate different
 * versions of the same algorithm. There are multiple impls.
 * of union find due to performance optimizations.
 * Some benchmarks exist to measure the difference between
 * forkjoin & queue approaches and huge/heavy
 */
public enum UnionFindAlgorithmType implements UnionFindAlgorithm {

    QUEUE {
        @Override
        public GraphUnionFindAlgo<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final GraphUnionFindAlgo.Config config,
                final AllocationTracker tracker) {

            return new ParallelUnionFindQueue(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker);
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return ParallelUnionFindQueue.memoryEstimation(incremental);
        }
    },
    FORK_JOIN {
        @Override
        public GraphUnionFindAlgo<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final GraphUnionFindAlgo.Config config,
                final AllocationTracker tracker) {

            return new ParallelUnionFindForkJoin(
                    graph,
                    tracker,
                    minBatchSize,
                    concurrency,
                    config);
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return ParallelUnionFindForkJoin.memoryEstimation(incremental);
        }
    },
    FJ_MERGE {
        @Override
        public GraphUnionFindAlgo<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final GraphUnionFindAlgo.Config config,
                final AllocationTracker tracker) {

            return new ParallelUnionFindFJMerge(
                    graph,
                    executor,
                    tracker,
                    minBatchSize,
                    concurrency,
                    config);
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return ParallelUnionFindFJMerge.memoryEstimation(incremental);
        }
    },
    SEQ {
        @Override
        public GraphUnionFindAlgo<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final GraphUnionFindAlgo.Config config,
                final AllocationTracker tracker) {

            return new GraphUnionFind(
                    graph,
                    config,
                    AllocationTracker.EMPTY
            );
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return GraphUnionFind.memoryEstimation(incremental);
        }
    },
}
