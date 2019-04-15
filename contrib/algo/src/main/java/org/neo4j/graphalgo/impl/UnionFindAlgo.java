/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;

import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

/**
 * this class is basically a helper to instantiate different
 * versions of the same algorithm. There are multiple impls.
 * of union find due to performance optimizations.
 * Some benchmarks exist to measure the difference between
 * forkjoin & queue approaches and huge/heavy
 */
public enum UnionFindAlgo {

    QUEUE {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            ParallelUnionFindQueue algo = new ParallelUnionFindQueue(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(ParallelUnionFindQueue)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeParallelUnionFindQueue algo = new HugeParallelUnionFindQueue(
                    hugeGraph,
                    executor,
                    minBatchSize,
                    concurrency,
                    tracker);
            prepare.accept("CC(HugeParallelUnionFindQueue)", algo);
            PagedDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    },
    FORK_JOIN {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            ParallelUnionFindForkJoin algo = new ParallelUnionFindForkJoin(
                    graph,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(ParallelUnionFindForkJoin)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeParallelUnionFindForkJoin algo = new HugeParallelUnionFindForkJoin(
                    hugeGraph,
                    tracker,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(HugeParallelUnionFindForkJoin)", algo);
            PagedDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    },
    FJ_MERGE {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            ParallelUnionFindFJMerge algo = new ParallelUnionFindFJMerge(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(ParallelUnionFindFJMerge)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeParallelUnionFindFJMerge algo = new HugeParallelUnionFindFJMerge(
                    hugeGraph,
                    executor,
                    tracker,
                    minBatchSize,
                    concurrency);
            prepare.accept("CC(HugeParallelUnionFindFJMerge)", algo);
            PagedDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    },
    SEQ {
        @Override
        DSSResult run(
                Graph graph,
                ExecutorService executor,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            GraphUnionFind algo = new GraphUnionFind(graph);
            prepare.accept("CC(SequentialUnionFind)", algo);
            DisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }

        @Override
        DSSResult run(
                HugeGraph hugeGraph,
                ExecutorService executor,
                AllocationTracker tracker,
                int minBatchSize,
                int concurrency,
                double threshold,
                BiConsumer<String, Algorithm<?>> prepare) {
            HugeGraphUnionFind algo = new HugeGraphUnionFind(
                    hugeGraph,
                    AllocationTracker.EMPTY);
            prepare.accept("CC(HugeSequentialUnionFind)", algo);
            PagedDisjointSetStruct struct = Double.isFinite(threshold)
                    ? algo.compute(threshold)
                    : algo.compute();
            algo.release();
            return new DSSResult(struct);
        }
    };

    public static BiConsumer<String, Algorithm<?>> NOTHING = (s, a) -> {
    };

    abstract DSSResult run(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare);

    abstract DSSResult run(
            HugeGraph hugeGraph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare);

    public final DSSResult runAny(
            Graph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency,
            double threshold,
            BiConsumer<String, Algorithm<?>> prepare) {
        if (graph instanceof HugeGraph) {
            HugeGraph hugeGraph = (HugeGraph) graph;
            return run(
                    hugeGraph,
                    executor,
                    tracker,
                    minBatchSize,
                    concurrency,
                    threshold,
                    prepare);
        } else {
            return run(
                    graph,
                    executor,
                    minBatchSize,
                    concurrency,
                    threshold,
                    prepare);
        }
    }

    public final DSSResult run(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency) {
        return run(
                graph,
                executor,
                minBatchSize,
                concurrency,
                Double.NaN,
                NOTHING);
    }

    public final DSSResult run(
            HugeGraph hugeGraph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        return run(
                hugeGraph,
                executor,
                tracker,
                minBatchSize,
                concurrency,
                Double.NaN,
                NOTHING
        );
    }
}
