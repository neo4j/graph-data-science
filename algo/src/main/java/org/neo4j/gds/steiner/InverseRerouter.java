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
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.PRUNED;
import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.ROOT_NODE;

public class InverseRerouter extends ReroutingAlgorithm {

    private final BitSet isTerminal;
    private final HugeLongArray examinationQueue;
    private final LongAdder indexQueue;
    private final ProgressTracker progressTracker;

    InverseRerouter(
        Graph graph,
        long sourceId,
        BitSet isTerminal,
        HugeLongArray examinationQueue,
        LongAdder indexQueue,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(graph, sourceId, concurrency, progressTracker);
        this.isTerminal = isTerminal;
        this.examinationQueue = examinationQueue;
        this.indexQueue = indexQueue;
        this.progressTracker = progressTracker;
    }

    @Override
    public void reroute(
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    ) {
        progressTracker.beginSubTask("Reroute");

        LinkCutTree linkCutTree = createLinkCutTree(parent);
        ReroutingChildrenManager childrenManager = new ReroutingChildrenManager(
            graph.nodeCount(),
            isTerminal,
            sourceId
        );

        initializeChildrenManager(childrenManager, parent);
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.max(graph.nodeCount());
        HugeLongArray currentSegmentArray = HugeLongArray.newArray(graph.nodeCount());
        HugeLongArrayQueue examinationQueue = HugeLongArrayQueue.newQueue(graph.nodeCount());
        HugeLongArray pruningArray = HugeLongArray.newArray(graph.nodeCount());
        HugeLongArray bestAlternative = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray bestAlternativeParentCost = HugeDoubleArray.newArray(graph.nodeCount());

        long currentSegmentIndex = 0;
        long endIndex = indexQueue.longValue();
        //we start traversing paths to terminals in the order they were found
        for (long indexId = 0; indexId < endIndex; ++indexId) {
            long element = this.examinationQueue.get(indexId);

            if (element != PRUNED) { //PRUNED signals end of a path
                currentSegmentArray.set(currentSegmentIndex++, element);
                continue;
            }

            while (currentSegmentIndex > 0) {
                long node = currentSegmentArray.get(--currentSegmentIndex);
                examinationQueue.add(node); //transfer path to the examination queue (FIFO)
                //if the node in the path cannot be pruned (because it's part of other paths), we prune what we can so far
                //at the end prune
                boolean shouldOptimizeSegment = currentSegmentIndex == 0 || !childrenManager.prunable(node);

                if (shouldOptimizeSegment) {
                    optimizeSegment(
                        childrenManager,
                        examinationQueue,
                        priorityQueue,
                        pruningArray,
                        bestAlternative,
                        bestAlternativeParentCost,
                        linkCutTree,
                        parent,
                        parentCost,
                        totalCost,
                        effectiveNodeCount
                    );
                }
            }

        }
        progressTracker.endSubTask("Reroute");
    }
    private void initializeChildrenManager(ReroutingChildrenManager childrenManager, HugeLongArray parent) {
        graph.forEachNode(nodeId -> {
            var parentId = parent.get(nodeId);
            if (parentId != PRUNED && parentId != ROOT_NODE) {
                childrenManager.link(nodeId, parentId);
            }
            return true;
        });
    }

    private void optimizeSegment(
        ReroutingChildrenManager childrenManager,
        HugeLongArrayQueue examinationQueue,
        HugeLongPriorityQueue priorityQueue,
        HugeLongArray pruningArray,
        HugeLongArray bestAlternative,
        HugeDoubleArray bestAlternativeParentCost,
        LinkCutTree linkCutTree,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount

    ) {
        long stopPruningNode = -1;
        double pruningGain = 0;

        long curr = examinationQueue.peek();

        //the parent of the path could have had many children (which made it not-runed)
        //but these children might have been removed, which could perhaps make it prunable now
        //so when we include the  cost of pruning a node from  the current path, we might be able to gain some more!
        while (childrenManager.prunable(parent.get(curr))) { //source is always reachable and never prunable
            var nextCurr = parent.get(curr);
            var nextCost = parentCost.get(nextCurr);
            pruningGain += nextCost;
            curr = nextCurr;
        }

        while (!examinationQueue.isEmpty()) {
            long nodeId = examinationQueue.remove();

            var parentId = parent.get(nodeId);

            if (stopPruningNode == -1) {
                stopPruningNode = parent.get(curr);
            }

            pruningGain += parentCost.get(nodeId);  //if we cut nodeId, we get rid of extra parentCost(nodeId) cost
            pruningArray.set(nodeId, 0);
            bestAlternative.set(nodeId, PRUNED);

            double finalPruningGain = pruningGain;
            //find the best node we can make it as parent
            double gain = processNodeInverseIndexedGraph(
                parent,
                bestAlternative,
                bestAlternativeParentCost,
                linkCutTree,
                nodeId,
                parentId,
                finalPruningGain
            );
            
            if (bestAlternative.get(nodeId) != PRUNED) { //add to the priority Queue
                priorityQueue.add(nodeId, gain);
            }
            progressTracker.logProgress();
        }
        double prunedSoFar = 0;
        while (!priorityQueue.isEmpty()) {
            long node = priorityQueue.top();
            if (node != PRUNED) { //node is still alive
                //we prune from top to bottom: prunedSoFar will include gain from nodes already pruned
                double adjustedCost = priorityQueue.cost(node) - prunedSoFar;
                if (adjustedCost > 0) { //maybe it's not worth pruning anymore
                    //if still worthy, we must check we are not creating a cycle
                    boolean canReroute = checkIfRerouteIsValid(
                        linkCutTree,
                        bestAlternative.get(node),
                        node,
                        parent.get(node)
                    );
                    if (canReroute) {
                        //if everything is alright, do pruning
                        prunedSoFar += rerouteWithPruning(
                            childrenManager,
                            bestAlternative,
                            bestAlternativeParentCost,
                            linkCutTree,
                            parent,
                            parentCost,
                            totalCost,
                            stopPruningNode,
                            node,
                            effectiveNodeCount
                        );

                        stopPruningNode = node; //node becomes the new stop, everything above it has been pruned in the current segment

                    } else {
                        //not ok, restore tree
                        linkCutTree.link(parent.get(node), node);
                    }
                }
            }
            priorityQueue.pop();
        }

    }

    private double processNodeInverseIndexedGraph(
        HugeLongArray parent,
        HugeLongArray bestAlternative,
        HugeDoubleArray bestAlternativeParentCost,
        LinkCutTree linkCutTree,
        long nodeId,
        long parentId,
        double finalPruningGain
    ) {
        MutableDouble bestGain = new MutableDouble(-1);
        graph.forEachInverseRelationship(nodeId, 1.0, (s, t, w) -> {
            if (parent.get(nodeId) == t) {
                return true;
            }

            //  t must still be alive, in addition if we prune, we should be able to get a benefit from it
            //benefit is:  new edge cost - all edges pruned
            if (parent.get(t) != PRUNED && (finalPruningGain - w) > bestGain.doubleValue()) {
                //now we must check that t is not a descendant of s.
                boolean canReconnect = checkIfRerouteIsValid(linkCutTree, t, nodeId, parentId);
                if (canReconnect) {
                    bestAlternative.set(nodeId, t);
                    bestGain.setValue(finalPruningGain - w);
                    bestAlternativeParentCost.set(
                        nodeId,
                        w
                    ); //this can be deduced via subtraction later on, but let's keep it
                }
                //we always restore the tree to its original state
                linkCutTree.link(parentId, nodeId);

            }
                return true;
            }
        );
        return bestGain.doubleValue();
    }
    
    private double rerouteWithPruning(
        ReroutingChildrenManager childrenManager,
        HugeLongArray bestAlternative,
        HugeDoubleArray bestAlternativeParentCost,
        LinkCutTree linkCutTree,
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        long stopPruningNode,
        long node,
        LongAdder effectiveNodeCount
    ) {
        double pruned = 0;
        long current = node;
        //start pruning until you hit the stop
        while (stopPruningNode != current) {

            long nextCurrent = parent.get(current);
            childrenManager.cut(current);  //with pruning we remove some useless nodes
            parent.set(current, PRUNED);
            double nodePruneCost = parentCost.get(current);
            pruned += nodePruneCost;
            totalCost.add(-nodePruneCost); //and their associaed cost
            parentCost.set(current, PRUNED);
            current = nextCurrent;
            effectiveNodeCount.decrement(); //this also cuts the node we are going to reroute....
        }
        effectiveNodeCount.increment(); //...hence we need do a +1 to recover it
        linkCutTree.link(bestAlternative.get(node), node); //now we link node to its new parent
        childrenManager.link(node, bestAlternative.get(node));
        parentCost.set(node, bestAlternativeParentCost.get(node));  //and adjust its cost
        parent.set(node, bestAlternative.get(node));
        totalCost.add(parentCost.get(node)); //and the overall cost

        return pruned;
    }
}
