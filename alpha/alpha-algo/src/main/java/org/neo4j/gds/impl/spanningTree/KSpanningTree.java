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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipProperties;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

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
public class KSpanningTree extends Algorithm<SpanningTree> {

    private IdMap idMap;
    private Graph graph;
    private RelationshipProperties weights;
    private final DoubleUnaryOperator minMax;
    private final long startNodeId;
    private final long k;

    private SpanningTree spanningTree;

    public KSpanningTree(
        IdMap idMap,
        Graph graph,
        RelationshipProperties weights,
        DoubleUnaryOperator minMax,
        long startNodeId,
        long k,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.idMap = idMap;
        this.graph = graph;
        this.weights = weights;
        this.minMax = minMax;
        this.startNodeId = (int) graph.toMappedNodeId(startNodeId);

        this.k = k;
    }

    @Override
    public SpanningTree compute() {
        progressTracker.beginSubTask();
        Prim prim = new Prim(
            idMap,
            graph,
            minMax,
            graph.toOriginalNodeId(startNodeId),
            progressTracker
        );
        prim.setTerminationFlag(getTerminationFlag());
        SpanningTree spanningTree = prim.compute();
        HugeLongArray parent = spanningTree.parent;
        long parentSize = parent.size();
        HugeLongPriorityQueue priorityQueue = minMax == Prim.MAX_OPERATOR ? HugeLongPriorityQueue.min(parentSize) : HugeLongPriorityQueue.max(
            parentSize);
        progressTracker.beginSubTask(parentSize);
        for (long i = 0; i < parentSize && terminationFlag.running(); i++) {
            long p = parent.get(i);
            if (p == -1) {
                continue;
            }
            priorityQueue.add(i, weights.relationshipProperty(p, i, 0.0D));
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        progressTracker.beginSubTask(k - 1);
        // remove k-1 relationships
        for (long i = 0; i < k - 1 && terminationFlag.running(); i++) {
            long cutNode = priorityQueue.pop();
            parent.set(cutNode, -1);
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        this.spanningTree = prim.getSpanningTree();
        progressTracker.endSubTask();
        return this.spanningTree;
    }

    @Override
    public void release() {
        idMap = null;
        graph = null;
        weights = null;
        spanningTree = null;
    }
}
