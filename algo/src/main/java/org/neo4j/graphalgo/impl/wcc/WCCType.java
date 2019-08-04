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
package org.neo4j.graphalgo.impl.wcc;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;

public enum WCCType {

    PARALLEL {
        @Override
        public WCC<ParallelWCC> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final WCC.Config config,
                final AllocationTracker tracker) {
            return new ParallelWCC(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker);
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return ParallelWCC.memoryEstimation(incremental);
        }
    },
    FORK_JOIN {
        @Override
        public WCC<WCCForkJoin> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final WCC.Config config,
                final AllocationTracker tracker) {
            return new WCCForkJoin(
                    graph,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker);
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return WCCForkJoin.memoryEstimation(incremental);
        }
    },
    FJ_MERGE {
        @Override
        public WCC<WCCForkJoinMerge> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final WCC.Config config,
                final AllocationTracker tracker) {
            return new WCCForkJoinMerge(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker);
        }

        @Override
        public MemoryEstimation memoryEstimation(boolean incremental) {
            return WCCForkJoinMerge.memoryEstimation(incremental);
        }
    },
    PREGEL {
        @Override
        public UnionFind<? extends UnionFind> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final UnionFind.Config config,
                final AllocationTracker tracker) {
            return new UnionFindPregel(graph, executor, minBatchSize, concurrency, config, tracker);
        }

        @Override
        MemoryEstimation memoryEstimation(final boolean incremental) {
            return UnionFindPregel.memoryEstimation(incremental);
        }
    };

    public abstract WCC<? extends WCC<?>> create(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            final WCC.Config config,
            AllocationTracker tracker);

    MemoryEstimation memoryEstimation() {
        return memoryEstimation(false);
    }

    abstract MemoryEstimation memoryEstimation(final boolean incremental);
}
