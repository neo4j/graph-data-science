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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public class ShortestPathsSteinerAlgorithm extends Algorithm<SteinerTreeResult> {

    public static long ROOTNODE = -1;
    public static long PRUNED = -2;
    private final Graph graph;
    private final long sourceId;
    private final List<Long> terminals;
    private final int concurrency;
    private final BitSet isTerminal;
    private final boolean applyRerouting;
    private final double delta;
    private final ExecutorService executorService;
    private int binSizeThreshold;

    public ShortestPathsSteinerAlgorithm(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        double delta,
        int concurrency,
        boolean applyRerouting,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sourceId = sourceId;
        this.terminals = terminals;
        this.concurrency = concurrency;
        this.delta = delta;
        this.isTerminal = createTerminals();
        this.applyRerouting = applyRerouting;
        this.executorService = executorService;
        this.binSizeThreshold = SteinerBasedDeltaStepping.BIN_SIZE_THRESHOLD;
    }

    @TestOnly
    ShortestPathsSteinerAlgorithm(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        double delta,
        int concurrency,
        boolean applyRerouting,
        int binSizeThreshold,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sourceId = sourceId;
        this.terminals = terminals;
        this.concurrency = concurrency;
        this.delta = delta;
        this.isTerminal = createTerminals();
        this.applyRerouting = applyRerouting;
        this.executorService = executorService;
        this.binSizeThreshold = binSizeThreshold;
    }

    private BitSet createTerminals() {
        long maxTerminalId = -1;
        for (long terminalId : terminals) {
            if (terminalId > maxTerminalId) {
                maxTerminalId = terminalId;
            }
        }
        BitSet terminalBitSet = new BitSet(maxTerminalId + 1);
        for (long terminalId : terminals) {
            terminalBitSet.set(terminalId);
        }
        return terminalBitSet;
    }

    @Override
    public SteinerTreeResult compute() {
        progressTracker.beginSubTask("SteinerTree");
        if (applyRerouting) {
            progressTracker.beginSubTask("Main");
        }
        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray parentCost = HugeDoubleArray.newArray(graph.nodeCount());
        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency, v -> {
            parentCost.set(v, PRUNED);
            parent.set(v, PRUNED);
        });
        DoubleAdder totalCost = new DoubleAdder();
        LongAdder effectiveNodeCount = new LongAdder();
        LongAdder terminalsReached = new LongAdder();

        effectiveNodeCount.increment(); //sourceNode is always in the solution
        var shortestPaths = runShortestPaths();

        initForSource(parent, parentCost);

        shortestPaths.forEachPath(path -> {
            processPath(path, parent, parentCost, totalCost, effectiveNodeCount);
            terminalsReached.increment();
        });

        if (applyRerouting) {
            progressTracker.endSubTask("Main");
            reroute(parent, parentCost, totalCost, effectiveNodeCount);
        }
        progressTracker.endSubTask("SteinerTree");
        return SteinerTreeResult.of(
            parent,
            parentCost,
            totalCost.doubleValue(),
            effectiveNodeCount.longValue(),
            terminalsReached.longValue()
        );
    }

    @Override
    public void release() {

    }

    private void initForSource(HugeLongArray parent, HugeDoubleArray parentCost) {
        parent.set(sourceId, ROOTNODE);
        parentCost.set(sourceId, 0);
    }

    private void processPath(
        PathResult path,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    ) {

        long targetId = path.targetNode();

        if (isTerminal.get(targetId)) {
            var ids = path.nodeIds();
            var costs = path.costs();
            var pastLength = costs.length;
            totalCost.add(path.totalCost());
            for (int j = pastLength - 1; j >= 0; --j) {
                long nodeId = ids[j + 1];
                long parentId = ids[j];
                double cost = costs[j];
                if (j > 0) {
                    cost -= costs[j - 1];
                }
                parent.set(nodeId, parentId);
                parentCost.set(nodeId, cost);
                effectiveNodeCount.increment();

            }
        }
    }

    private DijkstraResult runShortestPaths() {

        var steinerBasedDelta = new SteinerBasedDeltaStepping(
            graph,
            sourceId,
            delta,
            isTerminal,
            concurrency,
            binSizeThreshold,
            executorService,
            progressTracker
        );

        return steinerBasedDelta.compute();

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
            if (parentId != PRUNED && parentId != ROOTNODE) {
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
            if (parent.get(nodeId) != PRUNED && parent.get(nodeId) != ROOTNODE) {
                if (!endsAtTerminal.get(nodeId)) {
                    parent.set(nodeId, PRUNED);
                    totalCost.add(-parentCost.get(nodeId));
                    parentCost.set(nodeId, PRUNED);
                    effectiveNodeCount.decrement();
                }
            }
        });

    }

    private void reroute(
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    ) {
        progressTracker.beginSubTask("Rerouting");
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
                    if (parentId != PRUNED && parentId != ROOTNODE && w < targetParentCost) {
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
        progressTracker.endSubTask("Rerouting");
    }


}
