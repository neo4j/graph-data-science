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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * this class is basically a helper to instantiate different
 * versions of the same algorithm. There are multiple impls.
 * of union find due to performance optimizations.
 * Some benchmarks exist to measure the difference between
 * forkjoin & queue approaches and huge/heavy
 */
public enum UnionFindAlgo implements UnionFindAlgoInterface {

    QUEUE {
        @Override
        public GraphUnionFindAlgo<?> algo(
                final Optional<Graph> graph,
                final ExecutorService executor,
                final AllocationTracker tracker,
                final int minBatchSize,
                final int concurrency) {

            return new ParallelUnionFindQueue(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    tracker);
        }
    },
    FORK_JOIN {
        @Override
        public GraphUnionFindAlgo<?> algo(
                final Optional<Graph> graph,
                final ExecutorService executor,
                final AllocationTracker tracker,
                final int minBatchSize,
                final int concurrency) {

            return new ParallelUnionFindForkJoin(
                    graph,
                    tracker,
                    minBatchSize,
                    concurrency);
        }
    },
    FJ_MERGE {
        @Override
        public GraphUnionFindAlgo<?> algo(
                final Optional<Graph> graph,
                final ExecutorService executor,
                final AllocationTracker tracker,
                final int minBatchSize,
                final int concurrency) {

            return new ParallelUnionFindFJMerge(
                    graph,
                    executor,
                    tracker,
                    minBatchSize,
                    concurrency);
        }
    },
    SEQ {
        @Override
        public GraphUnionFindAlgo<?> algo(
                final Optional<Graph> graph,
                final ExecutorService executor,
                final AllocationTracker tracker,
                final int minBatchSize,
                final int concurrency) {

            return new GraphUnionFind(
                    graph,
                    AllocationTracker.EMPTY);
        }
    }
}
