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

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

class UnweightedForwardTraverser implements ForwardTraverser {

    static UnweightedForwardTraverser create(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        TerminationFlag terminationFlag
    ) {
        var nodeCount = graph.nodeCount();
        var distances = HugeIntArray.newArray(nodeCount);
        distances.fill(-1);
        var nodeQueue = HugeLongArrayQueue.newQueue(nodeCount);
        return new UnweightedForwardTraverser(
            graph,
            predecessors,
            backwardNodes,
            sigma,
            nodeQueue,
            distances,
            terminationFlag
        );
    }

    private final Graph graph;
    private final HugeObjectArray<LongArrayList> predecessors;
    private final HugeLongArrayStack backwardNodes;
    private final HugeLongArray sigma;
    private final HugeLongArrayQueue nodeQueue;
    private final HugeIntArray distances;
    private final TerminationFlag terminationFlag;

    UnweightedForwardTraverser(
        Graph graph,
        HugeObjectArray<LongArrayList> predecessors,
        HugeLongArrayStack backwardNodes,
        HugeLongArray sigma,
        HugeLongArrayQueue nodeQueue,
        HugeIntArray distances,
        TerminationFlag terminationFlag
    ) {
        this.graph = graph;
        this.predecessors = predecessors;
        this.backwardNodes = backwardNodes;
        this.sigma = sigma;
        this.nodeQueue = nodeQueue;
        this.distances = distances;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public void traverse(long startNodeId) {
        distances.set(startNodeId, 0);
        nodeQueue.add(startNodeId);

        while (!nodeQueue.isEmpty() && terminationFlag.running()) {
            long node = nodeQueue.remove();
            backwardNodes.push(node);
            int distanceNode = distances.get(node);

            graph.forEachRelationship(node, (source, target) -> {
                if (distances.get(target) < 0) {
                    nodeQueue.add(target);
                    distances.set(target, distanceNode + 1);
                }

                if (distances.get(target) == distanceNode + 1) {
                    sigma.addTo(target, sigma.get(source));
                    append(target, source);
                }
                return true;
            });
        }
    }

    @Override
    public void clear() {
        distances.fill(-1);
    }

    // append node to the path at target
    private void append(long target, long node) {
        LongArrayList targetPredecessors = predecessors.get(target);
        if (null == targetPredecessors) {
            targetPredecessors = new LongArrayList();
            predecessors.set(target, targetPredecessors);
        }
        targetPredecessors.add(node);
    }

}
