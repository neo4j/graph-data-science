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
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;

public enum UnionFindType {

    QUEUE {
        @Override
        public UnionFind<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final UnionFind.Config config,
                final AllocationTracker tracker,
                final Log log) {

            return new UnionFindQueue(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker,
                    log);
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return UnionFindQueue.memoryEstimation(incremental);
        }
    },
    FORK_JOIN {
        @Override
        public UnionFind<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final UnionFind.Config config,
                final AllocationTracker tracker,
                final Log log) {

            return new UnionFindForkJoin(
                    graph,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker,
                    log
            );
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return UnionFindForkJoin.memoryEstimation(incremental);
        }
    },
    FJ_MERGE {
        @Override
        public UnionFind<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final UnionFind.Config config,
                final AllocationTracker tracker,
                final Log log) {

            return new UnionFindFJMerge(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    config,
                    tracker,
                    log);
        }

        @Override
        public MemoryEstimation memoryEstimation(boolean incremental) {
            return UnionFindFJMerge.memoryEstimation(incremental);
        }
    },
    SEQ {
        @Override
        public UnionFind<?> create(
                final Graph graph,
                final ExecutorService executor,
                final int minBatchSize,
                final int concurrency,
                final UnionFind.Config config,
                final AllocationTracker tracker,
                final Log log) {

            return new UnionFindSeq(
                    graph,
                    config,
                    AllocationTracker.EMPTY,
                    log
            );
        }

        @Override
        public MemoryEstimation memoryEstimation(final boolean incremental) {
            return UnionFindSeq.memoryEstimation(incremental);
        }
    };

    public abstract UnionFind<?> create(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            final UnionFind.Config config,
            AllocationTracker tracker,
            final Log log);

    MemoryEstimation memoryEstimation() {
        return memoryEstimation(false);
    }

    abstract MemoryEstimation memoryEstimation(final boolean incremental);
}
