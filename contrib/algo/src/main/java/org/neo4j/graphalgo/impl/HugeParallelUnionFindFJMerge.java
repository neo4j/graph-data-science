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

import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

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
public class HugeParallelUnionFindFJMerge extends GraphUnionFindAlgo<HugeGraph, PagedDisjointSetStruct, HugeParallelUnionFindFJMerge> {

    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;
    private PagedDisjointSetStruct struct;

    /**
     * initialize UF
     *
     * @param graph
     * @param executor
     */
    HugeParallelUnionFindFJMerge(
            HugeGraph graph,
            ExecutorService executor,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        super(graph);
        this.executor = executor;
        this.tracker = tracker;
        nodeCount = graph.nodeCount();
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);
    }

    public PagedDisjointSetStruct compute() {

        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge(ufProcesses);
        return getStruct();
    }

    public PagedDisjointSetStruct compute(double threshold) {
        final Collection<TUFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return getStruct();
    }

    private void merge(Collection<? extends UFTask> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        if (!running()) {
            return;
        }
        final Stack<PagedDisjointSetStruct> temp = new Stack<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct()));
        struct = ForkJoinPool.commonPool().invoke(new Merge(temp));
    }

    public PagedDisjointSetStruct getStruct() {
        return struct;
    }

    @Override
    public HugeParallelUnionFindFJMerge release() {
        struct = null;
        return super.release();
    }

    private abstract class UFTask implements Runnable {
        abstract PagedDisjointSetStruct struct();
    }

    /**
     * Process for finding unions of weakly connected components
     */
    private class UFProcess extends UFTask {

        private final long offset;
        private final long end;
        private final PagedDisjointSetStruct struct;
        private final HugeRelationshipIterator rels;

        UFProcess(long offset, long length) {
            this.offset = offset;
            this.end = offset + length;
            struct = new PagedDisjointSetStruct(nodeCount, tracker).reset();
            rels = graph.concurrentCopy();
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                try {
                    rels.forEachRelationship(
                            node,
                            Direction.OUTGOING,
                            (sourceNodeId, targetNodeId) -> {
                                struct.union(sourceNodeId, targetNodeId);
                                return true;
                            });
                } catch (Exception e) {
                    throw ExceptionUtil.asUnchecked(e);
                }
            }
            getProgressLogger().logProgress((end - 1) / (nodeCount - 1));
        }

        @Override
        PagedDisjointSetStruct struct() {
            return struct;
        }
    }

    /**
     * Process to calc a DSS using a threshold
     */
    private class TUFProcess extends UFTask {

        private final long offset;
        private final long end;
        private final PagedDisjointSetStruct struct;
        private final HugeRelationshipIterator rels;
        private final double threshold;

        TUFProcess(long offset, long length, double threshold) {
            this.offset = offset;
            this.end = offset + length;
            this.threshold = threshold;
            struct = new PagedDisjointSetStruct(nodeCount, tracker).reset();
            rels = graph.concurrentCopy();
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            double weight = graph.weightOf(
                                    sourceNodeId,
                                    targetNodeId);
                            if (weight > threshold) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
        }

        @Override
        PagedDisjointSetStruct struct() {
            return struct;
        }
    }

    private class Merge extends RecursiveTask<PagedDisjointSetStruct> {

        private final Stack<PagedDisjointSetStruct> structs;

        private Merge(Stack<PagedDisjointSetStruct> structs) {
            this.structs = structs;
        }

        @Override
        protected PagedDisjointSetStruct compute() {
            final int size = structs.size();
            if (size == 1) {
                return structs.pop();
            }
            if (!running()) {
                return structs.pop();
            }
            if (size == 2) {
                return structs.pop().merge(structs.pop());
            }
            final Stack<PagedDisjointSetStruct> list = new Stack<>();
            list.push(structs.pop());
            list.push(structs.pop());
            final Merge mergeA = new Merge(structs);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final PagedDisjointSetStruct computed = mergeB.compute();
            return mergeA.join().merge(computed);
        }
    }


}
