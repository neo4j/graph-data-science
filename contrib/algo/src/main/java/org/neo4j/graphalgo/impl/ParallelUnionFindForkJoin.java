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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.Function;

/**
 * parallel UnionFind using common ForkJoin-Pool.
 * <p>
 * Implementation based on the idea that DisjointSetStruct can be built using
 * just a partition of the nodes which then can be merged pairwise.
 * <p>
 * The UnionFindTask splits a partition if its size is too big or calculates
 * its connected components otherwise.
 *
 * @author mknblch
 */
public class ParallelUnionFindForkJoin extends GraphUnionFindAlgo<Graph, DisjointSetStruct, ParallelUnionFindForkJoin> {

    private final int nodeCount;
    private final int batchSize;

    public static Function<Graph, ParallelUnionFindForkJoin> of(int minBatchSize, int concurrency) {
        return graph -> new ParallelUnionFindForkJoin(
                graph,
                minBatchSize,
                concurrency);
    }

    /**
     * initialize parallel UF
     *
     * @param graph
     */
    public ParallelUnionFindForkJoin(
            Graph graph,
            int minBatchSize,
            int concurrency) {
        super(graph);
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                concurrency,
                minBatchSize);

    }

    public DisjointSetStruct compute() {
        return ForkJoinPool.commonPool().invoke(new UnionFindTask(0));
    }


    public DisjointSetStruct compute(double threshold) {
        return ForkJoinPool
                .commonPool()
                .invoke(new ThresholdUFTask(0, threshold));
    }

    /**
     * basic union find task, takes a node from its partition
     * and invokes the union method on every connected node.
     * resumes until no more nodes are left.
     */
    private class UnionFindTask extends RecursiveTask<DisjointSetStruct> {

        protected final int offset;
        protected final int end;

        UnionFindTask(int offset) {
            this.offset = offset;
            this.end = Math.min(offset + batchSize, nodeCount);
        }

        @Override
        protected DisjointSetStruct compute() {
            // split nodes
            if (nodeCount - end >= batchSize && running()) {
                final UnionFindTask process = new UnionFindTask(end);
                process.fork();
                return run().merge(process.join());
            }
            return run();
        }

        protected DisjointSetStruct run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId, relationId) -> {
                            if (!struct.connected(sourceNodeId, targetNodeId)) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
            getProgressLogger().logProgress(end - 1, nodeCount - 1);

            return struct;
        }
    }

    /**
     * the threshold uf task works most like
     * the normal UF Task with a precondition
     * for joining 2 nodes into a set.
     * the condition is just a comparision of the
     * weight of the relationship and a static threshold
     */
    private class ThresholdUFTask extends UnionFindTask {

        private final double threshold;

        ThresholdUFTask(int offset, double threshold) {
            super(offset);
            this.threshold = threshold;
        }

        protected DisjointSetStruct run() {
            final DisjointSetStruct struct = new DisjointSetStruct(nodeCount).reset();
            for (int node = offset; node < end && running(); node++) {
                graph.forEachRelationship(
                        node,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId, relationId, weight) -> {
                            if (weight < threshold) {
                                return true;
                            }
                            if (!struct.connected(sourceNodeId, targetNodeId)) {
                                struct.union(sourceNodeId, targetNodeId);
                            }
                            return true;
                        });
            }
            return struct;
        }
    }
}
