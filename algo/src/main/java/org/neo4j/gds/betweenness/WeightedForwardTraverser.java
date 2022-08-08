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
        this.graph = graph;
        this.predecessors = predecessors;
        this.backwardNodes = backwardNodes;
        this.sigma = sigma;
        this.nodeQueue = nodeQueue;
        this.visited = visited;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public void traverse(long startNodeId) {
        nodeQueue.add(startNodeId, 0.0D);

        while (!nodeQueue.isEmpty() && terminationFlag.running()) {
            var node = nodeQueue.top();
            backwardNodes.push(node);
            var nodeCost = nodeQueue.cost(node);
            nodeQueue.pop();
            visited.set(node);

            graph.forEachRelationship(
                node,
                1.0D,
                (source, target, weight) -> {
                    if (visited.get(target)) {
                        return true;
                    }
                    var targetCost = nodeCost + weight;
                    boolean firstTime = !nodeQueue.containsElement(target);
                    if (firstTime) {
                        nodeQueue.add(target, targetCost);
                    }

                    var storedTargetCost = nodeQueue.cost(target);
                    if (Double.compare(targetCost, storedTargetCost) == 0) {
                        sigma.addTo(target, sigma.get(source));
                        appendPredecessor(target, node);
                    } else if (Double.compare(targetCost, storedTargetCost) < 0) {
                        nodeQueue.set(target, targetCost);
                        sigma.set(target, sigma.get(source));
                        var targetPredecessors = predecessors.get(target);
                        targetPredecessors.clear();
                        targetPredecessors.add(source);
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

    // append node to the path at target
    private void appendPredecessor(long target, long node) {
        LongArrayList targetPredecessors = predecessors.get(target);
        if (null == targetPredecessors) {
            targetPredecessors = new LongArrayList();
            predecessors.set(target, targetPredecessors);
        }
        targetPredecessors.add(node);
    }
}
