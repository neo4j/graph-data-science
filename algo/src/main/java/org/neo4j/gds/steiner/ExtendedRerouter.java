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

import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.PRUNED;
import static org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm.ROOT_NODE;

public class ExtendedRerouter extends ReroutingAlgorithm {

    private final BitSet isTerminal;
    private final HugeLongArray examinationQueue;
    private final LongAdder indexQueue;

    ExtendedRerouter(
        Graph graph,
        long sourceId,
        List<Long> terminals,
        BitSet isTerminal,
        HugeLongArray examinationQueue,
        LongAdder indexQueue,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(graph, sourceId, terminals, concurrency, progressTracker);
        this.isTerminal = isTerminal;
        this.examinationQueue = examinationQueue;
        this.indexQueue = indexQueue;
    }

    @Override
    public void reroute(
        HugeLongArray parent,
        HugeDoubleArray parentCost,
        DoubleAdder totalCost,
        LongAdder effectiveNodeCount
    ) {
        LinkCutTree linkCutTree = createLinkCutTree(parent);
        ReroutingChildrenManager childrenManager = new ReroutingChildrenManager(graph.nodeCount(), isTerminal);
        initializeChildrenManager(childrenManager, parent);
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.max(graph.nodeCount());
        HugeLongArray wipArray = HugeLongArray.newArray(graph.nodeCount());
        HugeLongArrayQueue examinationQueue = HugeLongArrayQueue.newQueue(graph.nodeCount());
        HugeLongArray pruningArray = HugeLongArray.newArray(graph.nodeCount());
        HugeLongArray bestAlternative = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray bestAlternativeParentCost = HugeDoubleArray.newArray(graph.nodeCount());
        long wipIndex = 0;
        long endIndex = indexQueue.longValue();
        for (long indexId = 0; indexId < endIndex; ++indexId) {
            boolean reachedSegmentEnd = false;
            long element = this.examinationQueue.get(indexId);
            if (element == PRUNED) {
                reachedSegmentEnd = true;
            } else {
                wipArray.set(wipIndex++, element);

            } //3 2 1
            if (reachedSegmentEnd) {
                while (wipIndex > 0) {
                    long node = wipArray.get(--wipIndex);
                    examinationQueue.add(node);
                    boolean shouldOptimizeSegment = wipIndex == 0 || !childrenManager.prunable(node);
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
        }
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
        System.out.println("segment: ");
        while (!examinationQueue.isEmpty()) {
            long nodeId = examinationQueue.remove();
            System.out.print(nodeId + " ");
            var parentId = parent.get(nodeId);

            if (stopPruningNode == -1) {
                stopPruningNode = parentId;
            }
            pruningGain += parentCost.get(nodeId);
            pruningArray.set(nodeId, 0);
            bestAlternative.set(nodeId, PRUNED);
            double finalPruningGain = pruningGain;
            double gain = processNodeInReverseGraph(
                parent,
                bestAlternative,
                bestAlternativeParentCost,
                linkCutTree,
                nodeId,
                parentId,
                finalPruningGain
            );
            if (bestAlternative.get(nodeId) != PRUNED) { //add to the priority Queue
                System.out.println("    " + nodeId + " " + gain);
                priorityQueue.add(nodeId, gain);
            }
        }
        System.out.println();
        double prunedSoFar = 0;
        while (!priorityQueue.isEmpty()) {
            long node = priorityQueue.top();
            if (node != PRUNED) { //node is still alive
                double adjustedCost = priorityQueue.cost(node) - prunedSoFar;
                if (adjustedCost > 0) {
                    boolean canReroute = checkIfRerouteIsValid(
                        linkCutTree,
                        bestAlternative.get(node),
                        node,
                        parent.get(node)
                    );
                    if (canReroute) {
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

                        stopPruningNode = node;

                    } else {
                        linkCutTree.link(parent.get(node), node);
                    }
                }
            }
            priorityQueue.pop();
        }

    }
    private double processNodeInReverseGraph(
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
            System.out.println("    looking at : " + nodeId + " -->" + t + "[" + finalPruningGain + "]" + " " + w);

            //  t must still be alive, in addition if we prune, we should be able to get a benefit from it
            if (t != PRUNED && (finalPruningGain - w) > bestGain.doubleValue()) {
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
        });
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
        while (stopPruningNode != current) {
            long nextCurrent = parent.get(current);
            childrenManager.cut(current);
            parent.set(current, PRUNED);
            double nodePruneCost = parentCost.get(current);
            pruned += nodePruneCost;
            totalCost.add(-nodePruneCost);
            parentCost.set(current, PRUNED);
            current = nextCurrent;
            effectiveNodeCount.decrement();
        }
        effectiveNodeCount.increment();
        linkCutTree.link(bestAlternative.get(node), node);
        childrenManager.link(node, bestAlternative.get(node));
        parentCost.set(node, bestAlternativeParentCost.get(node));
        parent.set(node, bestAlternative.get(node));
        totalCost.add(parentCost.get(node));
        return pruned;
    }
}
