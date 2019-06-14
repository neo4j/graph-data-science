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
import org.neo4j.graphalgo.core.utils.paged.PagedDisjointSetStruct;
import org.neo4j.graphdb.Direction;

import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * parallel UnionFind using common ForkJoin-Pool only.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The UnionFindTask extracts a nodePartition if its input-set is too big and
 * calculates its result while lending the rest-nodeSet to another FJ-Task.
 * <p>
 * Note: The splitting method might be sub-optimal since the resulting work-tree is
 * very unbalanced so each thread needs to wait for its predecessor to complete
 * before merging his set into the parent set.
 *
 * @author mknblch
 */
public class ParallelUnionFindForkJoin extends GraphUnionFindAlgo<ParallelUnionFindForkJoin>
{

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(ParallelUnionFindForkJoin.class)
            .startField("computeStep", ThresholdUFTask.class)
            .add(MemoryEstimations.of("dss", (dimensions, concurrency) ->
                    PagedDisjointSetStruct.memoryRequirements()
                            .apply(dimensions, concurrency)
                            .memoryUsage()
                            .times(concurrency)))
            .endField()
            .build();

    private final AllocationTracker tracker;
    private final long nodeCount;
    private final long batchSize;

    /**
     * initialize parallel UF
     *
     * @param graph
     */
    public ParallelUnionFindForkJoin(
            Optional<Graph> graph,
            AllocationTracker tracker,
            int minBatchSize,
            int concurrency) {
        super(graph.orElse(null));

        if (graph.isPresent()) {
            nodeCount = graph.get().nodeCount();
        } else {
            nodeCount = 0;
        }

        this.tracker = tracker;
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);

    }

    public PagedDisjointSetStruct compute() {
        return ForkJoinPool.commonPool().invoke(new UnionFindTask(0));
    }


    public PagedDisjointSetStruct compute(double threshold) {
        return ForkJoinPool
                .commonPool()
                .invoke(new ThresholdUFTask(0, threshold));
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    private class UnionFindTask extends RecursiveTask<PagedDisjointSetStruct> {

        private final long offset;
        private final long end;
        private final RelationshipIterator rels;

        UnionFindTask(long offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
            this.rels = graph.concurrentCopy();
        }

        @Override
        protected PagedDisjointSetStruct compute() {
            if (nodeCount - end >= batchSize && running()) {
                final UnionFindTask process = new UnionFindTask(end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected PagedDisjointSetStruct run() {
            final PagedDisjointSetStruct struct = new PagedDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            struct.union(sourceNodeId, targetNodeId);
                            return true;
                        });
            }
            getProgressLogger().logProgress(end - 1, nodeCount - 1);

            return struct;
        }
    }

    private class ThresholdUFTask extends RecursiveTask<PagedDisjointSetStruct> {

        private final long offset;
        private final long end;
        private final RelationshipIterator rels;
        private final double threshold;

        ThresholdUFTask(long offset, double threshold) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
            this.rels = graph.concurrentCopy();
            this.threshold = threshold;
        }

        @Override
        protected PagedDisjointSetStruct compute() {
            if (nodeCount - end >= batchSize && running()) {
                final ThresholdUFTask process = new ThresholdUFTask(
                        offset,
                        end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected PagedDisjointSetStruct run() {
            final PagedDisjointSetStruct struct = new PagedDisjointSetStruct(
                    nodeCount,
                    tracker).reset();
            for (long node = offset; node < end && running(); node++) {
                rels.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (source, target) -> {
                            double weight = graph.weightOf(source, target);
                            if (weight >= threshold && !struct.connected(
                                    source,
                                    target)) {
                                struct.union(source, target);
                            }
                            return true;
                        });
            }
            return struct;
        }
    }
}
