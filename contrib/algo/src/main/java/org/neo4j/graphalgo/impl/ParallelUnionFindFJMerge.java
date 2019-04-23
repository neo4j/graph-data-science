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
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.dss.DisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;

/**
 * parallel UnionFind using ExecutorService and common ForkJoin-Pool.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * Like in {@link ParallelUnionFindForkJoin} the resulting DSS of each node-partition
 * is merged by the ForkJoin pool while calculating the DSS is done by the
 * ExecutorService.
 * <p>
 * This might lead to a better distribution of tasks in the merge-tree.
 *
 * @author mknblch
 */
public class ParallelUnionFindFJMerge extends GraphUnionFindAlgo<Graph, DisjointSetStruct, ParallelUnionFindFJMerge> {

    private final ExecutorService executor;
    private final int nodeCount;
    private final int batchSize;
    private DisjointSetStruct struct;

    public static Function<Graph, ParallelUnionFindFJMerge> of(ExecutorService executor, int minBatchSize, int concurrency) {
        return graph -> new ParallelUnionFindFJMerge(
                graph,
                executor,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize UF
     *
     * @param graph
     * @param executor
     */
    public ParallelUnionFindFJMerge(Graph graph, ExecutorService executor, int minBatchSize, int concurrency) {
        super(graph);
        this.executor = executor;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency, minBatchSize);
    }

    public DisjointSetStruct compute() {

        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge(ufProcesses);
        return getStruct();
    }

    public DisjointSetStruct compute(double threshold) {
        final ArrayList<TUFProcess> ufProcesses = new ArrayList<>();
        for (int i = 0; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return getStruct();
    }

    public void merge(ArrayList<? extends UFProcess> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        if (!running()) {
            return;
        }
        final Stack<DisjointSetStruct> temp = new Stack<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct));
        struct = ForkJoinPool.commonPool().invoke(new Merge(temp));
    }

    public DisjointSetStruct getStruct() {
        return struct;
    }

    @Override
    public ParallelUnionFindFJMerge release() {
        struct = null;
        return super.release();
    }

    /**
     * Process for finding unions of weakly connected components
     */
    private class UFProcess implements Runnable {

        protected final int offset;
        protected final int end;
        protected final DisjointSetStruct struct;

        public UFProcess(int offset, int length) {
            this.offset = offset;
            this.end = offset + length;
            struct = new DisjointSetStruct(nodeCount).reset();
        }

        @Override
        public void run() {
            for (int node = offset; node < end && node < nodeCount && running(); node++) {
                try {
                    graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
                        if (!struct.connected(sourceNodeId, targetNodeId)) {
                            struct.union(sourceNodeId, targetNodeId);
                        }
                        return true;
                    });
                } catch (Exception e) {
                    throw ExceptionUtil.asUnchecked(e);
                }
            }
            getProgressLogger().logProgress((end - 1) / (nodeCount - 1));
        }
    }

    /**
     * Process to calc a DSS using a threshold
     */
    private class TUFProcess extends UFProcess {

        private final double threshold;

        public TUFProcess(int offset, int length, double threshold) {
            super(offset, length);
            this.threshold = threshold;
        }

        @Override
        public void run() {
            for (int node = offset; node < end && node < nodeCount && running(); node++) {
                graph.forEachRelationship(node, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
                    if (weight > threshold) {
                        struct.union(sourceNodeId, targetNodeId);
                    }
                    return true;
                });
            }
        }
    }

    private class Merge extends RecursiveTask<DisjointSetStruct> {

        private final Stack<DisjointSetStruct> structs;

        private Merge(Stack<DisjointSetStruct> structs) {
            this.structs = structs;
        }

        @Override
        protected DisjointSetStruct compute() {
            final int size = structs.size();
            if (size == 1) {
                return structs.pop();
            }
            if (!running()) {
                return structs.pop();
            }
            if (size == 2) {
                return merge(structs.pop(), structs.pop());
            }
            final Stack<DisjointSetStruct> list = new Stack<>();
            list.push(structs.pop());
            list.push(structs.pop());
            final Merge mergeA = new Merge(structs);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final DisjointSetStruct computed = mergeB.compute();
            return merge(mergeA.join(), computed);
        }

        private DisjointSetStruct merge(DisjointSetStruct a, DisjointSetStruct b) {
            return a.merge(b);
        }
    }


}
