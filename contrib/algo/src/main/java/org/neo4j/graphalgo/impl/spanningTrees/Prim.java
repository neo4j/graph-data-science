/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.queue.SharedIntPriorityQueue;
import org.neo4j.graphalgo.core.utils.traverse.SimpleBitSet;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
 * Sequential Single-Source minimum/maximum weight spanning tree algorithm (PRIM).
 *
 * @author mknblch
 */
public class Prim extends Algorithm<Prim> {

    private final RelationshipIterator relationshipIterator;
    private final RelationshipWeights weights;
    private final int nodeCount;

    private SpanningTree spanningTree;

    public Prim(IdMapping idMapping, RelationshipIterator relationshipIterator, RelationshipWeights weights) {
        this.relationshipIterator = relationshipIterator;
        this.weights = weights;
        nodeCount = Math.toIntExact(idMapping.nodeCount());
    }

    public Prim computeMaximumSpanningTree(int startNode) {
        this.spanningTree = prim(startNode, true);
        return this;
    }

    public Prim computeMinimumSpanningTree(int startNode) {
        this.spanningTree = prim(startNode, false);
        return this;
    }

    /**
     * callculate min or max spanning trees
     * @param startNode the start node
     * @param max true to calc max spanning tree, false to calc min spanning tree
     * @return
     */
    private SpanningTree prim(int startNode, boolean max) {
        final int[] parent = new int[nodeCount];
        final IntDoubleMap cost = new IntDoubleScatterMap(nodeCount);
        final SharedIntPriorityQueue queue = SharedIntPriorityQueue.min(
                nodeCount,
                cost,
                Double.MAX_VALUE);
        final ProgressLogger logger = getProgressLogger();
        final SimpleBitSet visited = new SimpleBitSet(nodeCount);
        Arrays.fill(parent, -1);
        cost.put(startNode, 0.0);
        queue.add(startNode, -1.0);
        int effectiveNodeCount = 0;
        while (!queue.isEmpty() && running()) {
            final int node = queue.pop();
            if (visited.contains(node)) {
                continue;
            }
            effectiveNodeCount++;
            visited.put(node);
            relationshipIterator.forEachRelationship(node, Direction.OUTGOING, (s, t, r) -> {
                if (visited.contains(t)) {
                    return true;
                }
                // invert weight to calculate maximum
                final double w = max ? -weights.weightOf(s, t) : weights.weightOf(s, t);
                if (w < cost.getOrDefault(t, Double.MAX_VALUE)) {
                    if (cost.containsKey(t)) {
                        cost.put(t, w);
                        queue.update(t);
                    } else {
                        cost.put(t, w);
                        queue.add(t, -1.0);
                    }
                    parent[t] = s;
                }
                return true;
            });
            logger.logProgress(effectiveNodeCount, nodeCount - 1);
        }
        return new SpanningTree(startNode, nodeCount, effectiveNodeCount, parent);
    }

    public SpanningTree getSpanningTree() {
        return spanningTree;
    }

    @Override
    public Prim me() {
        return this;
    }

    @Override
    public Prim release() {
        spanningTree = null;
        return this;
    }

    public static class Result {

        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long effectiveNodeCount;

        public Result(long loadMillis,
                      long computeMillis,
                      long writeMillis,
                      int effectiveNodeCount) {
            this.loadMillis = loadMillis;
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
            return new Result(loadDuration,
                    evalDuration,
                    writeDuration,
                    effectiveNodeCount);
        }
    }

}
