/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.impl.spanningTree;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.function.DoubleUnaryOperator;

/**
 * Sequential Single-Source minimum weight spanning tree algorithm (PRIM).
 * <p>
 * The algorithm computes the MST by traversing all nodes from a given
 * startNodeId. It aggregates all transitions into a MinPriorityQueue
 * and visits each (unvisited) connected node by following only the
 * cheapest transition and adding it to a specialized form of undirected tree.
 * <p>
 * The algorithm also computes the minimum, maximum and sum of all
 * weights in the MST.
 */
public class Prim extends Algorithm<SpanningTree> {

    public static final DoubleUnaryOperator MAX_OPERATOR = (w) -> -w;
    public static final DoubleUnaryOperator MIN_OPERATOR = (w) -> w;
    private final Graph graph;
    private final int nodeCount;
    private final DoubleUnaryOperator minMax;
    private final int startNodeId;

    private SpanningTree spanningTree;

    public Prim(
        IdMap idMap,
        Graph graph,
        DoubleUnaryOperator minMax,
        long startNodeId,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = Math.toIntExact(idMap.nodeCount());
        this.minMax = minMax;
        this.startNodeId = (int) graph.toMappedNodeId(startNodeId);
    }

    @Override
    public SpanningTree compute() {
        progressTracker.beginSubTask(graph.nodeCount());
        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());

        HugeLongPriorityQueue queue = HugeLongPriorityQueue.min(nodeCount);
        BitSet visited = new BitSet(nodeCount);
        parent.fill(-1);

        queue.add(startNodeId, 0.0);
        long effectiveNodeCount = 0;
        while (!queue.isEmpty() && terminationFlag.running()) {
            long node = queue.pop();
            if (visited.get(node)) {
                continue;
            }
            effectiveNodeCount++;
            visited.set(node);
            graph.forEachRelationship(node, 0.0D, (s, t, w) -> {
                if (visited.get(t)) {
                    return true;
                }
                // invert weight to calculate maximum
                double weight = minMax.applyAsDouble(w);
                if (!queue.containsElement(t)) {
                    queue.add(t, weight);
                    parent.set(t, s);

                } else if (Double.compare(weight, queue.cost(t)) < 0) {
                    queue.set(t, weight);
                    parent.set(t, s);
                }


                return true;
            });
            progressTracker.logProgress();
        }
        this.spanningTree = new SpanningTree(startNodeId, nodeCount, effectiveNodeCount, parent);
        progressTracker.endSubTask();
        return this.spanningTree;
    }

    public SpanningTree getSpanningTree() {
        return spanningTree;
    }

    @Override
    public void release() {
        spanningTree = null;
    }

    public static class Result {

        public final long preProcessingMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long effectiveNodeCount;

        public Result(
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            long effectiveNodeCount
        ) {
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.effectiveNodeCount = effectiveNodeCount;
        }
    }

    public static class Builder extends AbstractResultBuilder<Result> {

        protected long effectiveNodeCount;

        public Builder withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        public Result build() {
            return new Result(
                preProcessingMillis,
                computeMillis,
                writeMillis,
                effectiveNodeCount
            );
        }
    }
}
