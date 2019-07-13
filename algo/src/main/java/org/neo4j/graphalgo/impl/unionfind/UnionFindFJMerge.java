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
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.IncrementalDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.RankedDisjointSetStruct;
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
 * Like in {@link UnionFindForkJoin} the resulting DSS of each node-partition
 * is merged by the ForkJoin pool while calculating the DSS is done by the
 * ExecutorService.
 * <p>
 * This might lead to a better distribution of tasks in the merge-tree.
 *
 * @author mknblch
 */
public class UnionFindFJMerge extends UnionFind<UnionFindFJMerge> {

    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;
    private DisjointSetStruct dss;

    public static MemoryEstimation memoryEstimation(final boolean incremental) {
        return MemoryEstimations.builder(UnionFindFJMerge.class)
                .startField("computeStep", TUFProcess.class)
                .add(MemoryEstimations.of("DisjointSetStruct", (dimensions, concurrency) -> {
                            MemoryEstimation dssEstimation = (incremental) ?
                                    IncrementalDisjointSetStruct.memoryEstimation() :
                                    RankedDisjointSetStruct.memoryEstimation();
                            return dssEstimation
                                    .estimate(dimensions, concurrency)
                                    .memoryUsage()
                                    .times(concurrency);
                        }
                ))
                .endField()
                .build();
    }

    /**
     * initialize UF
     *
     * @param graph
     * @param executor
     */
    public UnionFindFJMerge(
            Graph graph,
            ExecutorService executor,
            int minBatchSize,
            int concurrency,
            UnionFind.Config algoConfig,
            AllocationTracker tracker) {
        super(graph, algoConfig);

        this.nodeCount = graph.nodeCount();
        this.executor = executor;
        this.tracker = tracker;
        this.dss = initDisjointSetStruct(nodeCount, tracker);
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);
    }

    @Override
    public DisjointSetStruct compute(double threshold) {
        final ArrayList<TUFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new TUFProcess(i, batchSize, threshold));
        }
        merge(ufProcesses);
        return getDss();
    }

    @Override
    public DisjointSetStruct computeUnrestricted() {
        final ArrayList<UFProcess> ufProcesses = new ArrayList<>();
        for (long i = 0L; i < nodeCount; i += batchSize) {
            ufProcesses.add(new UFProcess(i, batchSize));
        }
        merge(ufProcesses);
        return getDss();
    }

    private void merge(Collection<? extends UFTask> ufProcesses) {
        ParallelUtil.run(ufProcesses, executor);
        if (!running()) {
            return;
        }
        final Stack<DisjointSetStruct> temp = new Stack<>();
        ufProcesses.forEach(uf -> temp.add(uf.struct()));
        dss = ForkJoinPool.commonPool().invoke(new Merge(temp));
    }

    public DisjointSetStruct getDss() {
        return dss;
    }

    @Override
    public UnionFindFJMerge release() {
        dss = null;
        return super.release();
    }

    private abstract class UFTask implements Runnable {
        abstract DisjointSetStruct struct();
    }

    /**
     * Process for finding unions of weakly connected components
     */
    private class UFProcess extends UFTask {

        private final long offset;
        private final long end;
        private final DisjointSetStruct struct;
        private final RelationshipIterator rels;

        UFProcess(long offset, long length) {
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
        DisjointSetStruct struct() {
            return struct;
        }
    }

    /**
     * Process to calc a DSS using a threshold
     */
    private class TUFProcess extends UFTask {

        private final long offset;
        private final long end;
        private final DisjointSetStruct struct;
        private final RelationshipIterator rels;
        private final double threshold;

        TUFProcess(long offset, long length, double threshold) {
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
        DisjointSetStruct struct() {
            return struct;
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
                return structs.pop().merge(structs.pop());
            }
            final Stack<DisjointSetStruct> list = new Stack<>();
            list.push(structs.pop());
            list.push(structs.pop());
            final Merge mergeA = new Merge(structs);
            final Merge mergeB = new Merge(list);
            mergeA.fork();
            final DisjointSetStruct computed = mergeB.compute();
            return mergeA.join().merge(computed);
        }
    }


}
