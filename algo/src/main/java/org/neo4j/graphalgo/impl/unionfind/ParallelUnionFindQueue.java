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
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.IncrementalDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.RankedDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.awaitTermination;

/**
 * parallel UnionFind using ExecutorService only.
 * <p>
 * Algorithm based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The implementation is based on a queue which acts as a buffer
 * for each computed DSS. As long as there are more elements on
 * the queue the algorithm takes two, merges them and adds its
 * result to the queue until only 1 element remains.
 *
 * @author mknblch
 */
public class ParallelUnionFindQueue extends GraphUnionFindAlgo<ParallelUnionFindQueue> {

    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;
    private final int stepSize;

    public static MemoryEstimation memoryEstimation(final boolean incremental) {
        return MemoryEstimations
                .builder(ParallelUnionFindQueue.class)
                .startField("computeStep", HugeUnionFindTask.class)
                .add(MemoryEstimations.of("DisjointSetStruct", (dimensions, concurrency) -> {
                    MemoryEstimation dssEstimation = (incremental) ?
                            IncrementalDisjointSetStruct.memoryEstimation() :
                            RankedDisjointSetStruct.memoryEstimation();
                    return dssEstimation
                            .estimate(dimensions, concurrency)
                            .memoryUsage()
                            .times(concurrency);
                }))
                .endField()
                .build();
    }

    /**
     * Initialize parallel UF.
     */
    public ParallelUnionFindQueue(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            GraphUnionFind.Config algoConfig,
            AllocationTracker tracker) {
        super(graph, algoConfig);
        this.executor = executor;
        this.tracker = tracker;

        this.nodeCount = graph.nodeCount();

        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize,
                Integer.MAX_VALUE);
        long targetSteps = ParallelUtil.threadSize(batchSize, nodeCount);
        if (targetSteps > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "too many nodes (%d) to run union find with the given concurrency (%d) and batchSize (%d)",
                    nodeCount,
                    concurrency,
                    batchSize));
        }
        this.stepSize = (int) targetSteps;
    }

    @Override
    public DisjointSetStruct compute() {
        return computeUnrestricted();
    }

    @Override
    public DisjointSetStruct compute(double threshold) {
        throw new IllegalArgumentException(
                "Parallel UnionFind with threshold not implemented, please use either `concurrency:1` or one of the exp* variants of UnionFind");
    }

    @Override
    public DisjointSetStruct computeUnrestricted() {
        final List<Future<?>> futures = new ArrayList<>(2 * stepSize);
        final BlockingQueue<DisjointSetStruct> queue = new ArrayBlockingQueue<>(stepSize);
        AtomicInteger expectedStructs = new AtomicInteger();

        for (long i = 0L; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new HugeUnionFindTask(queue, i, expectedStructs)));
        }
        int steps = futures.size();

        for (int i = 1; i < steps; ++i) {
            futures.add(executor.submit(() -> mergeTask(queue, expectedStructs, DisjointSetStruct::merge)));
        }

        awaitTermination(futures);
        return getStruct(queue);
    }

    private DisjointSetStruct getStruct(final BlockingQueue<DisjointSetStruct> queue) {
        DisjointSetStruct set = queue.poll();
        if (set == null) {
            set = initDisjointSetStruct(nodeCount, tracker);
        }
        return set;
    }

    public static <T> void mergeTask(
            final BlockingQueue<T> queue,
            final AtomicInteger expected,
            BinaryOperator<T> merge) {
        // basically a decrement operation, but we don't decrement in case there's not
        // enough sets for us to operate on
        int available, afterMerge;
        do {
            available = expected.get();
            // see if there are at least two sets to take, so we don't wait for a set that will never come
            if (available < 2) {
                return;
            }
            // decrease by one, as we're pushing a new set onto the queue
            afterMerge = available - 1;
        } while (!expected.compareAndSet(available, afterMerge));

        boolean pushed = false;
        try {
            final T a = queue.take();
            final T b = queue.take();
            final T next = merge.apply(a, b);
            queue.add(next);
            pushed = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            if (!pushed) {
                expected.decrementAndGet();
            }
        }
    }

    private class HugeUnionFindTask implements Runnable {

        private final RelationshipIterator rels;
        private final BlockingQueue<DisjointSetStruct> queue;
        private final AtomicInteger expectedStructs;
        private final long offset;
        private final long end;

        HugeUnionFindTask(
                BlockingQueue<DisjointSetStruct> queue,
                long offset,
                AtomicInteger expectedStructs) {
            this.rels = graph.concurrentCopy();
            this.queue = queue;
            this.expectedStructs = expectedStructs;
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
            expectedStructs.incrementAndGet();
        }

        @Override
        public void run() {
            boolean pushed = false;
            try {
                final DisjointSetStruct struct = initDisjointSetStruct(nodeCount, tracker).reset();
                for (long node = offset; node < end; node++) {
                    rels.forEachRelationship(
                            node,
                            Direction.OUTGOING,
                            (sourceNodeId, targetNodeId) -> {
                                struct.union(sourceNodeId, targetNodeId);
                                return true;
                            });
                }
                getProgressLogger().logProgress((end - 1.0) / (nodeCount - 1.0));
                try {
                    queue.put(struct);
                    pushed = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } finally {
                if (!pushed) {
                    expectedStructs.decrementAndGet();
                }
            }
        }
    }
}
