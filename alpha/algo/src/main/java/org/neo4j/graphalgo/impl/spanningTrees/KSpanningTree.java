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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipProperties;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.container.UndirectedTree;
import org.neo4j.graphalgo.core.utils.queue.IntPriorityQueue;

import java.util.function.DoubleUnaryOperator;

/**
 * The algorithm computes the MST by traversing all nodes from a given
 * startNodeId. It aggregates all transitions into a MinPriorityQueue
 * and visits each (unvisited) connected node by following only the
 * cheapest transition and adding it to a specialized form of {@link UndirectedTree}.
 * <p>
 * After calculating the MST the algorithm cuts the tree at its k weakest
 * relationships to form k spanning trees
 */
public class KSpanningTree extends Algorithm<KSpanningTree, SpanningTree> {

    private IdMapping idMapping;
    private RelationshipIterator relationshipIterator;
    private RelationshipProperties weights;
    private final int nodeCount;
    private final DoubleUnaryOperator minMax;
    private final int startNodeId;
    private final long k;

    private SpanningTree spanningTree;

    public KSpanningTree(
        IdMapping idMapping,
        RelationshipIterator relationshipIterator,
        RelationshipProperties weights,
        DoubleUnaryOperator minMax,
        int startNodeId,
        long k
) {
        this.idMapping = idMapping;
        this.relationshipIterator = relationshipIterator;
        this.weights = weights;
        this.nodeCount = Math.toIntExact(idMapping.nodeCount());
        this.minMax = minMax;
        this.startNodeId = startNodeId;
        this.k = k;
    }

    @Override
    public SpanningTree compute() {
        ProgressLogger logger = getProgressLogger();
        Prim prim = new Prim(
            idMapping,
            relationshipIterator,
            minMax,
            startNodeId
        ).withProgressLogger(getProgressLogger())
            .withTerminationFlag(getTerminationFlag());

        IntPriorityQueue priorityQueue = minMax == Prim.MAX_OPERATOR ? IntPriorityQueue.min() : IntPriorityQueue.max();
        SpanningTree spanningTree = prim.compute();
        int[] parent = spanningTree.parent;
        for (int i = 0; i < parent.length && running(); i++) {
            int p = parent[i];
            if (p == -1) {
                continue;
            }
            priorityQueue.add(i, weights.relationshipProperty(p, i, 0.0D));
            logger.logProgress(i, nodeCount, () -> "reorganization");
        }
        // remove k-1 relationships
        for (int i = 0; i < k - 1 && running(); i++) {
            int cutNode = priorityQueue.pop();
            parent[cutNode] = -1;
        }
        this.spanningTree = prim.getSpanningTree();
        return this.spanningTree;
    }

    public SpanningTree getSpanningTree() {
        return spanningTree;
    }

    @Override
    public KSpanningTree me() {
        return this;
    }

    @Override
    public void release() {
        idMapping = null;
        relationshipIterator = null;
        weights = null;
        spanningTree = null;
    }
}
