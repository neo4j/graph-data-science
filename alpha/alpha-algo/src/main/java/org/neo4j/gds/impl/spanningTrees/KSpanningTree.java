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
package org.neo4j.gds.impl.spanningTrees;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMapping;
import org.neo4j.gds.api.RelationshipProperties;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.IntPriorityQueue;

import java.util.function.DoubleUnaryOperator;

/**
 * The algorithm computes the MST by traversing all nodes from a given
 * startNodeId. It aggregates all transitions into a MinPriorityQueue
 * and visits each (unvisited) connected node by following only the
 * cheapest transition and adding it to a specialized form of undirected tree.
 * <p>
 * After calculating the MST the algorithm cuts the tree at its k weakest
 * relationships to form k spanning trees
 */
public class KSpanningTree extends Algorithm<KSpanningTree, SpanningTree> {

    private IdMapping idMapping;
    private Graph graph;
    private RelationshipProperties weights;
    private final DoubleUnaryOperator minMax;
    private final int startNodeId;
    private final long k;

    private SpanningTree spanningTree;

    public KSpanningTree(
        IdMapping idMapping,
        Graph graph,
        RelationshipProperties weights,
        DoubleUnaryOperator minMax,
        long startNodeId,
        long k,
        ProgressTracker progressTracker
) {
        this.idMapping = idMapping;
        this.graph = graph;
        this.weights = weights;
        this.minMax = minMax;
        this.startNodeId = (int) graph.toMappedNodeId(startNodeId);

        this.k = k;
        this.progressTracker = progressTracker;
    }

    @Override
    public SpanningTree compute() {
        progressTracker.beginSubTask();
        Prim prim = new Prim(
            idMapping,
            graph,
            minMax,
            graph.toOriginalNodeId(startNodeId),
            progressTracker
        ).withTerminationFlag(getTerminationFlag());

        IntPriorityQueue priorityQueue = minMax == Prim.MAX_OPERATOR ? IntPriorityQueue.min() : IntPriorityQueue.max();
        SpanningTree spanningTree = prim.compute();
        int[] parent = spanningTree.parent;
        progressTracker.beginSubTask(parent.length);
        for (int i = 0; i < parent.length && running(); i++) {
            int p = parent[i];
            if (p == -1) {
                continue;
            }
            priorityQueue.add(i, weights.relationshipProperty(p, i, 0.0D));
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        progressTracker.beginSubTask(k - 1);
        // remove k-1 relationships
        for (int i = 0; i < k - 1 && running(); i++) {
            int cutNode = priorityQueue.pop();
            parent[cutNode] = -1;
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        this.spanningTree = prim.getSpanningTree();
        progressTracker.endSubTask();
        return this.spanningTree;
    }

    @Override
    public KSpanningTree me() {
        return this;
    }

    @Override
    public void release() {
        idMapping = null;
        graph = null;
        weights = null;
        spanningTree = null;
    }
}
