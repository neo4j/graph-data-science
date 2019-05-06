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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Function;

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
public class ParallelUnionFindQueue extends GraphUnionFindAlgo<Graph, DisjointSetStruct, ParallelUnionFindQueue> {

    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;

    public static Function<Graph, ParallelUnionFindQueue> of(ExecutorService executor, int minBatchSize, int concurrency) {
        return graph -> new ParallelUnionFindQueue(
                graph,
                executor,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize parallel UF
     */
    public ParallelUnionFindQueue(Graph graph, ExecutorService executor, int minBatchSize, int concurrency) {
        super(graph);
        this.executor = executor;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, minBatchSize);
    }

    @Override
    public DisjointSetStruct compute() {
        int stepSize = ParallelUtil.threadSize(batchSize, nodeCount);
        final List<Future<?>> futures = new ArrayList<>(2 * stepSize);
        final BlockingQueue<DisjointSetStruct> queue = new ArrayBlockingQueue<>(stepSize);
        AtomicInteger expectedStructs = new AtomicInteger();

        for (int i = 0; i < nodeCount; i += batchSize) {
            futures.add(executor.submit(new UnionFindTask(queue, i, expectedStructs)));
        }
        int steps = futures.size();

        for (int i = 1; i < steps; ++i) {
            futures.add(executor.submit(() -> mergeTask(queue, expectedStructs, DisjointSetStruct::merge)));
        }

        awaitTermination(futures);
        return getStruct(queue);
    }


    @Override
    public DisjointSetStruct compute(double threshold) {
        throw new IllegalArgumentException("Parallel UnionFind with threshold not implemented, please use either `concurrency:1` or one of the exp* variants of UnionFind");
    }

    private DisjointSetStruct getStruct(final BlockingQueue<DisjointSetStruct> queue) {
        DisjointSetStruct set = queue.poll();
        if (set == null) {
            set = new DisjointSetStruct(nodeCount);
        }
        return set;
    }

    private class UnionFindTask implements Runnable {

        private final BlockingQueue<DisjointSetStruct> queue;
        private final AtomicInteger expectedStructs;
        private final int offset;
        private final int end;


        UnionFindTask(
                BlockingQueue<DisjointSetStruct> queue,
                int offset,
                AtomicInteger expectedStructs) {
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
                final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
                for (int node = offset; node < end; node++) {
                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
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
