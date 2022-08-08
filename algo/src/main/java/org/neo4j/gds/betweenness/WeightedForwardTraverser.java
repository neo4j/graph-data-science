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
package org.neo4j.gds.betweenness;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

final class WeightedForwardTraverser implements ForwardTraverser {

    static WeightedForwardTraverser create(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        TerminationFlag terminationFlag
    ) {
        var nodeCount = graph.nodeCount();
        var nodeQueue = HugeLongPriorityQueue.min(nodeCount);
        var visited = new BitSet(nodeCount);
        return new WeightedForwardTraverser(
            graph,
            predecessors,
            backwardNodes,
            sigma,
            nodeQueue,
            visited,
            terminationFlag
        );
    }

    private final Graph graph;
    private final TerminationFlag terminationFlag;
    private final HugeLongArrayStack backwardNodes;
    private final HugeLongArray sigma;
    private final HugeLongPriorityQueue nodeQueue;
    private final HugeObjectArray<LongArrayList> predecessors;
    private final BitSet visited;

    private WeightedForwardTraverser(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        HugeLongPriorityQueue nodeQueue,
        BitSet visited,
        TerminationFlag terminationFlag
    ) {
        this.predecessors = predecessors;
        this.backwardNodes = backwardNodes;
        this.sigma = sigma;
        this.nodeQueue = nodeQueue;
        this.visited = visited;
        this.graph = graph;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public void traverse(long startNodeId) {
        nodeQueue.add(startNodeId, 0.0D);
        while (!nodeQueue.isEmpty() && terminationFlag.running()) {
            var node = nodeQueue.top();
            var thisNodesStoredCost = nodeQueue.cost(node);
            nodeQueue.pop();
            backwardNodes.push(node);
            visited.set(node);

            graph.forEachRelationship(
                node,
                1.0D,
                (source, target, weight) -> {
                    boolean visitedAlready = visited.get(target);
                    if (!visitedAlready) {
                        boolean insideQueue = nodeQueue.containsElement(target);
                        if (!insideQueue) {
                            nodeQueue.add(target, thisNodesStoredCost + weight);
                            var targetPredecessors = new LongArrayList();
                            predecessors.set(target, targetPredecessors);
                        }

                        if (nodeQueue.cost(target) == thisNodesStoredCost + weight) {
                            var pred = predecessors.get(target);
                            sigma.addTo(target, sigma.get(source));
                            pred.add(source);
                        } else if (weight + thisNodesStoredCost < nodeQueue.cost(target)) {
                            nodeQueue.set(target, weight + thisNodesStoredCost);
                            var pred = predecessors.get(target);
                            pred.clear();
                            sigma.set(target, sigma.get(source));
                            pred.add(source);
                        }
                    }
                    return true;
                }
            );
        }
    }

    @Override
    public void clear() {
        visited.clear();
    }
}
