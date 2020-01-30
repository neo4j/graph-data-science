/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.spanningTrees;

import com.carrotsearch.hppc.IntDoubleMap;
import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.container.SimpleBitSet;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.queue.SharedIntPriorityQueue;
import org.neo4j.graphalgo.results.AbstractResultBuilder;

import java.util.Arrays;
import java.util.function.DoubleUnaryOperator;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * Sequential Single-Source minimum weight spanning tree algorithm (PRIM).
 * <p>
 * The algorithm computes the MST by traversing all nodes from a given
 * startNodeId. It aggregates all transitions into a MinPriorityQueue
 * and visits each (unvisited) connected node by following only the
 * cheapest transition and adding it to a specialized form of {@link UndirectedTree}.
 * <p>
 * The algorithm also computes the minimum, maximum and sum of all
 * weights in the MST.
 */
public class Prim extends Algorithm<Prim, SpanningTree> {

    public static final DoubleUnaryOperator MAX_OPERATOR = (w) -> -w;
    public static final DoubleUnaryOperator MIN_OPERATOR = (w) -> w;
    private final RelationshipIterator relationshipIterator;
    private final int nodeCount;
    private final DoubleUnaryOperator minMax;
    private final int startNodeId;

    private SpanningTree spanningTree;

    public Prim(IdMapping idMapping, RelationshipIterator relationshipIterator, DoubleUnaryOperator minMax, int startNodeId) {
        this.relationshipIterator = relationshipIterator;
        this.nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.minMax = minMax;
        this.startNodeId = startNodeId;
    }

    @Override
    public SpanningTree compute() {
        int[] parent = new int[nodeCount];
        IntDoubleMap cost = new IntDoubleScatterMap(nodeCount);
        SharedIntPriorityQueue queue = SharedIntPriorityQueue.min(
                nodeCount,
                cost,
                Double.MAX_VALUE);
        ProgressLogger logger = getProgressLogger();
        SimpleBitSet visited = new SimpleBitSet(nodeCount);
        Arrays.fill(parent, -1);
        cost.put(startNodeId, 0.0);
        queue.add(startNodeId, -1.0);
        int effectiveNodeCount = 0;
        while (!queue.isEmpty() && running()) {
            int node = queue.pop();
            if (visited.contains(node)) {
                continue;
            }
            effectiveNodeCount++;
            visited.put(node);
            relationshipIterator.forEachRelationship(node, 0.0D, longToIntConsumer((s, t, w) -> {
                if (visited.contains(t)) {
                    return true;
                }
                // invert weight to calculate maximum
                double weight = minMax.applyAsDouble(w);
                if (weight < cost.getOrDefault(t, Double.MAX_VALUE)) {
                    if (cost.containsKey(t)) {
                        cost.put(t, weight);
                        queue.update(t);
                    } else {
                        cost.put(t, weight);
                        queue.add(t, -1.0);
                    }
                    parent[t] = s;
                }
                return true;
            }));
            logger.logProgress(effectiveNodeCount, nodeCount - 1);
        }
        this.spanningTree = new SpanningTree(startNodeId, nodeCount, effectiveNodeCount, parent);
        return this.spanningTree;
    }

    public SpanningTree getSpanningTree() {
        return spanningTree;
    }

    @Override
    public Prim me() {
        return this;
    }

    @Override
    public void release() {
        spanningTree = null;
    }

    public static class Result {

        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long effectiveNodeCount;

        public Result(long createMillis,
                      long computeMillis,
                      long writeMillis,
                      int effectiveNodeCount) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.effectiveNodeCount = effectiveNodeCount;
        }
    }

    public static class Builder extends AbstractResultBuilder<Result> {

        protected int effectiveNodeCount;

        public Builder withEffectiveNodeCount(int effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        public Result build() {
            return new Result(
                loadMillis,
                computeMillis,
                writeMillis,
                effectiveNodeCount);
        }
    }
}
