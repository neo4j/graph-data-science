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
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.PRUNED;
import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.ROOT_NODE;

public class SimpleRerouter  implements RerouteAlgorithm {

    private final Graph graph;
    private final ProgressTracker progressTracker;
    private final int concurrency;
    private final List<Long> terminals;
    private final long sourceId;
    public SimpleRerouter(Graph graph,
                          long sourceId,
                          List<Long> terminals,
                          int concurrency,
                          ProgressTracker progressTracker
    ){
        this.graph=graph;
        this.progressTracker=progressTracker;
        this.concurrency=concurrency;
        this.terminals=terminals;
        this.sourceId=sourceId;
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


    private void reconnect(
        LinkCutTree tree,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        long source,
        long target,
        double weight
    ) {
        double edgeCostOft = parentCost.get(target);
        parent.set(target, source);
        parentCost.set(target, weight);
        totalCost.add(-edgeCostOft + weight); //remove old cost, add new cost due to relinking
        tree.link(source, target); // update tree
    }

    private boolean checkIfRerouteIsValid(
        LinkCutTree tree,
        long source,
        long target,
        long parentTarget
    ) {
        //we want to check if the edge source->target causes a loop
        //i.e.,  target is a predecessor of source
        //just checking if source is connected to target is not valid (this is a spanning tree all are connected)
        //we use the LinkCutTree and cut the  target's parent. If the two are connected still, we'll have a loop
        //Example:
        //    pp->target-> a->b->->source
        // after cutting pp->target
        //  pp  target-> a->b->source
        // We have loop ==> so source->target is not a viable replacement

        //Otherwise, assuming no loop,  there is a root-source path not involving target. So by  setting source->target
        //all terminals with  target as predecessor can still reach be reached by source.
        tree.delete(parentTarget, target);
        return !tree.connected(source, target);
    }

    private LinkCutTree createLinkCutTree(HugeLongArray parent) {
        var tree = new LinkCutTree(graph.nodeCount());
        for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
            var parentId = parent.get(nodeId);
            if (parentId != PRUNED && parentId != ROOT_NODE) {
                tree.link(parentId, nodeId);
            }
        }
        return tree;
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

        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency, nodeId -> {
            if (parent.get(nodeId) != PRUNED && parent.get(nodeId) != ROOT_NODE) {
                if (!endsAtTerminal.get(nodeId)) {
                    parent.set(nodeId, PRUNED);
                    totalCost.add(-parentCost.get(nodeId));
                    parentCost.set(nodeId, PRUNED);
                    effectiveNodeCount.decrement();
                }
            }
        });

    }
}
