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
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.SequentialDisjointSetStruct;
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
 * Like in {@link WCCForkJoin} the resulting DSS of each node-partition
 * is merged by the ForkJoin pool while calculating the DSS is done by the
 * ExecutorService.
 * <p>
 * This might lead to a better distribution of tasks in the merge-tree.
 *
 * @author mknblch
 */
public class WCCForkJoinMerge extends WCC<WCCForkJoinMerge> {

    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;
    private SequentialDisjointSetStruct disjointSetStruct;

    public static MemoryEstimation memoryEstimation(final boolean incremental) {
        return WCC.memoryEstimation(incremental, WCCForkJoinMerge.class, ThresholdWCCProcess.class);
    }

    /**
     * initialize UF
     *
     * @param graph
     * @param executor
     */
    public WCCForkJoinMerge(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            WCC.Config algoConfig,
            AllocationTracker tracker) {
        super(graph, algoConfig);

        this.nodeCount = graph.nodeCount();
        this.executor = executor;
        this.tracker = tracker;
        this.disjointSetStruct = initDisjointSetStruct(nodeCount, tracker);
        this.batchSize = ParallelUtil.adjustedBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);
    }

    @Override
    public DisjointSetStruct compute(double threshold) {
        final ArrayList<ThresholdWCCProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new ThresholdWCCProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return disjointSetStruct;
    }

    @Override
    public DisjointSetStruct computeUnrestricted() {
        final ArrayList<WCCProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new WCCProcess(i, batchSize));
        }
        merge(ufProcesses);
        return disjointSetStruct;
    }

    private void merge(Collection<? extends AbstractWCCTask> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        if (!running()) {
            return;
        }
        final Stack<SequentialDisjointSetStruct> disjointSetStructs = new Stack<>();
        ufProcesses.forEach(uf -> disjointSetStructs.add(uf.getDisjointSetStruct()));
        disjointSetStruct = ForkJoinPool.commonPool().invoke(new Merge(disjointSetStructs));
    }

    @Override
    public void release() {
        disjointSetStruct = null;
    }

    private abstract static class AbstractWCCTask implements Runnable {
        abstract SequentialDisjointSetStruct getDisjointSetStruct();
    }

    /**
     * Process for finding unions of weakly connected components
     */
    private class WCCProcess extends AbstractWCCTask {

        private final long offset;
        private final long end;
        private final SequentialDisjointSetStruct struct;
        private final RelationshipIterator rels;

        WCCProcess(long offset, long length) {
            this.offset = offset;
            this.end = offset + length;
            struct = initDisjointSetStruct(nodeCount, tracker);
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
        SequentialDisjointSetStruct getDisjointSetStruct() {
            return struct;
        }
    }

    /**
     * Process to calc a DisjointSetStruct using a threshold
     */
    private class ThresholdWCCProcess extends AbstractWCCTask {

        private final long offset;
        private final long end;
        private final SequentialDisjointSetStruct struct;
        private final RelationshipIterator rels;
        private final double threshold;

        ThresholdWCCProcess(long offset, long length, double threshold) {
            this.offset = offset;
            this.end = offset + length;
            this.threshold = threshold;
            struct = initDisjointSetStruct(nodeCount, tracker);
            rels = graph.concurrentCopy();
        }

        @Override
        public void run() {
            for (long node = offset; node < end && node < nodeCount && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            double weight = graph.relationshipProperty(
                                    sourceNodeId,
                                    targetNodeId,
                                    WCC.defaultWeight(threshold));
                            if (weight > threshold) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
        }

        @Override
        SequentialDisjointSetStruct getDisjointSetStruct() {
            return struct;
        }
    }

    private class Merge extends RecursiveTask<SequentialDisjointSetStruct> {

        private final Stack<SequentialDisjointSetStruct> communityContainers;

        private Merge(Stack<SequentialDisjointSetStruct> structs) {
            this.communityContainers = structs;
        }

        @Override
        protected SequentialDisjointSetStruct compute() {
            final int size = communityContainers.size();
            if (size == 1) {
                return communityContainers.pop();
            }
            if (!running()) {
                return communityContainers.pop();
            }
            if (size == 2) {
                return communityContainers.pop().merge(communityContainers.pop());
            }
            final Stack<SequentialDisjointSetStruct> list = new Stack<>();
            list.push(communityContainers.pop());
            list.push(communityContainers.pop());
            final Merge mergeA = new Merge(communityContainers);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final SequentialDisjointSetStruct computed = mergeB.compute();
            return mergeA.join().merge(computed);
        }
    }


}
