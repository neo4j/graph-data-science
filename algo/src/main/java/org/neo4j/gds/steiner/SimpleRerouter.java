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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.PRUNED;
import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.ROOT_NODE;

public class SimpleRerouter extends ReroutingAlgorithm {

    private final List<Long> terminals;
    private final TerminationFlag terminationFlag;

    SimpleRerouter(
         Graph graph,
         long sourceId,
         List<Long> terminals,
         int concurrency,
         ProgressTracker progressTracker,
         TerminationFlag terminationFlag
     ) {
         super(graph, sourceId, concurrency, progressTracker);
         this.terminals = terminals;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public void reroute(
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    ) {
        progressTracker.beginSubTask("Reroute");
        //First, represent the tree as an LinkCutTree:
        // This is a dynamic tree (can answer connectivity like UnionFind)
        // but can also do some other cool stuff like answering path queries
        LinkCutTree tree = createLinkCutTree(parent);

        MutableBoolean didReroutes = new MutableBoolean();
        graph.forEachNode(nodeId -> {
            if (parent.get(nodeId) != PRUNED) {
                //we reroute only edges in the true
                graph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                    var parentId = parent.get(t);
                    double targetParentCost = parentCost.get(t);
                    //we want to see if replacing t's parent with nodeId makes sense (i.e., smaller cost)
                    if (parentId != PRUNED && parentId != ROOT_NODE && w < targetParentCost) {
                        boolean shouldReconnect = checkIfRerouteIsValid(tree, s, t, parentId);
                        if (shouldReconnect) { //yes we can replace t's parent with s
                            didReroutes.setTrue();
                            reconnect(tree, parent, parentCost, totalCost, s, t, w);
                        } else { //not possible, revert !
                            tree.link(parentId, t);
                        }
                    }
                    return true;
                });
            }
            progressTracker.logProgress();
            return true;
        });
        if (didReroutes.isTrue()) {
            cutNodesAfterRerouting(parent, parentCost, totalCost, effectiveNodeCount);
        }
        progressTracker.endSubTask("Reroute");
    }

    private void cutNodesAfterRerouting(
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    ) {
        BitSet endsAtTerminal = new BitSet(graph.nodeCount());
        HugeLongArrayQueue queue = HugeLongArrayQueue.newQueue(graph.nodeCount());
        for (var terminal : terminals) {
            if (parent.get(terminal) != PRUNED) {
                queue.add(terminal);
                endsAtTerminal.set(terminal);
            }
        }
        while (!queue.isEmpty()) {
            long nodeId = queue.remove();
            long parentId = parent.get(nodeId);
            if (parentId != sourceId && !endsAtTerminal.getAndSet(parentId)) {
                queue.add(parentId);
            }
        }

        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            terminationFlag,
            nodeId -> {
                if (parent.get(nodeId) != PRUNED && parent.get(nodeId) != ROOT_NODE) {
                    if (!endsAtTerminal.get(nodeId)) {
                        parent.set(nodeId, PRUNED);
                        totalCost.add(-parentCost.get(nodeId));
                        parentCost.set(nodeId, PRUNED);
                        effectiveNodeCount.decrement();
                    }
                }
            }
        );

    }
}
