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
package org.neo4j.gds.impl.spanningtree;

import com.carrotsearch.hppc.BitSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;

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

    private Graph graph;
    private final DoubleUnaryOperator minMax;
    private final long startNodeId;
    private final long k;

    private SpanningTree spanningTree;

    public KSpanningTree(
        Graph graph,
        DoubleUnaryOperator minMax,
        long startNodeId,
        long k,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.minMax = minMax;
        this.startNodeId = (int) graph.toMappedNodeId(startNodeId);

        this.k = k;
    }

    @Override
    public SpanningTree compute() {
        progressTracker.beginSubTask();
        Prim prim = new Prim(
            graph,
            minMax,
            startNodeId,
            progressTracker
        );

        prim.setTerminationFlag(getTerminationFlag());
        SpanningTree spanningTree = prim.compute();
        HugeLongArray parent = spanningTree.parentArray();
        long parentSize = parent.size();
        HugeLongPriorityQueue priorityQueue = createPriorityQueue(parentSize, true);

        progressTracker.beginSubTask(parentSize);
        for (long i = 0; i < parentSize && terminationFlag.running(); i++) {
            long p = parent.get(i);
            if (p == -1) {
                continue;
            }
            priorityQueue.add(i, spanningTree.costToParent(i));
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        progressTracker.beginSubTask(k - 1);
        // remove until there are k-1 relationships
        long numberOfDeletions = spanningTree.effectiveNodeCount() - k;
        for (long i = 0; i < numberOfDeletions && terminationFlag.running(); i++) {
            long cutNode = priorityQueue.pop();
            parent.set(cutNode, -1);
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
        this.spanningTree = prim.getSpanningTree();
        progressTracker.endSubTask();
        return this.spanningTree;
    }

    @NotNull
    private HugeLongPriorityQueue createPriorityQueue(long parentSize, boolean reverse) {
        //TODO: Don't forget to check this (something is wrong now but will fix tomorrowo)
        boolean condition = minMax == Prim.MAX_OPERATOR;
        if (reverse) {
            condition = !condition;
        }
        HugeLongPriorityQueue priorityQueue = condition
            ? HugeLongPriorityQueue.min(parentSize)
            : HugeLongPriorityQueue.max(parentSize);
        return priorityQueue;
    }

    @Override
    public void release() {
        graph = null;
        spanningTree = null;
    }

    private SpanningTree cutLeafApproach(SpanningTree spanningTree) {
        //this approach cuts a leaf at each step (remaining graph is always corrected)
        //so we can just cut the most expensive leaf at each step
        var priorityQueue = createPriorityQueue(graph.nodeCount(), true);
        HugeLongArray degree = HugeLongArray.newArray(graph.nodeCount());
        double startNodeRelationshipCost = -1.0;
        long startNodeSingleChild = -1;
        HugeLongArray parent = spanningTree.parentArray();

        for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
            var nodeParent = spanningTree.parent(nodeId);
            if (nodeParent != -1) {
                degree.set(nodeParent, degree.get(nodeParent) + 1);
                if (nodeParent == startNodeId) { //start-node needs special care because it's parent is -1
                    startNodeSingleChild = nodeId;
                    startNodeRelationshipCost = spanningTree.costToParent(nodeId);
                }
                degree.set(nodeId, degree.get(nodeId) + 1);
            }
        }

        for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
            if (degree.get(nodeId) == 1) {
                double relevantCost = (nodeId == startNodeId) ?
                    startNodeRelationshipCost : spanningTree.costToParent(nodeId);
                priorityQueue.add(nodeId, relevantCost);
            }
        }
        long numberOfDeletions = spanningTree.effectiveNodeCount() - k;

        for (long i = 0; i < numberOfDeletions; ++i) {
            var nextNode = priorityQueue.pop();
            long affectedNode = -1;
            if (nextNode == startNodeId) {
                parent.set(startNodeSingleChild, -1);
                affectedNode = startNodeSingleChild;
                //TODO: should also upd costArray
            } else {
                affectedNode = parent.get(nextNode);
                parent.set(nextNode, -1);
                //TODO: should also upd costArray
            }
            degree.set(affectedNode, degree.get(affectedNode) - 1);
            if (degree.get(affectedNode) == 1) {
                if (affectedNode != startNodeId) {
                    priorityQueue.add(affectedNode, spanningTree.costToParent(affectedNode));
                } else {
                    //this can only happen once so the O(n) cost is not a big overhead
                    for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
                        if (parent.get(nodeId) == startNodeId) {
                            priorityQueue.add(startNodeId, spanningTree.costToParent(nodeId));
                            startNodeSingleChild = nodeId;
                            break;
                        }
                    }
                }
            }
        }
        return spanningTree;
    }

    private SpanningTree growApproach(SpanningTree spanningTree) {

        //this approach grows gradually the MST found in the previous step
        //when it is about to get larger than K, we crop the current worst leaf if the new value to be added
        // is actually any smaller


        //TODO: Handle to be able to delete startNode as well (not much different from above approach)

        HugeLongArray outDegree = HugeLongArray.newArray(graph.nodeCount());

        HugeLongArray parent = spanningTree.parentArray();

        var priorityQueue = createPriorityQueue(graph.nodeCount(), false);
        var toTrim = createPriorityQueue(graph.nodeCount(), true);

        BitSet exterior = new BitSet(graph.nodeCount());
        priorityQueue.add(startNodeId, 0);
        long nodesInTree = 0;
        while (true) {
            long node = priorityQueue.pop();
            long nodeParent = spanningTree.parent(node);

            boolean nodeAdded = false;
            if (nodesInTree < k) { //if we are smaller, we can just add it no problemo
                nodesInTree++;
                nodeAdded = true;
            } else {
                while (!exterior.get(toTrim.top())) { //not valid frontier nodes anymore, just ignore
                    toTrim.pop();
                }
                boolean shouldMove = moveMakesSense(priorityQueue.cost(node), toTrim.cost(toTrim.top()), minMax);

                if (shouldMove && nodeParent != toTrim.top()) {
                    //we cannot add it, if it's parent is the one who we're going to kill next, right?
                    nodeAdded = true;
                    long popped = toTrim.pop();
                    long poppedPopps = parent.get(popped);
                    parent.set(popped, -1); //this guy bites the dust completely...
                    if (poppedPopps != -1) {
                        if (outDegree.get(poppedPopps) == 0) { //...and his parent might become open to deletion soon
                            toTrim.add(poppedPopps, spanningTree.costToParent(poppedPopps));
                            exterior.set(poppedPopps); //if so add it to the reverse p.q
                        }
                    }

                }
            }
            if (nodeAdded) {

                outDegree.set(nodeParent, outDegree.get(nodeParent) + 1);
                exterior.clear(nodeParent);

                graph.forEachRelationship(node, 1.0, (s, t, w) -> {
                    if (parent.get(t) == s) {
                        //TODO:  doing this only on the tree for now for simplicity
                        //cause its 18h and I do not want to think more :D
                        //might be able to work on all graph edges not just for those in the tree

                        if (!priorityQueue.containsElement(t)) {
                            priorityQueue.add(t, spanningTree.costToParent(t));
                        }

                    }
                    return true;
                });
            }
        }

    }

    private boolean moveMakesSense(double cost1, double cost2, DoubleUnaryOperator minMax) {
        if (minMax == Prim.MAX_OPERATOR) {
            return cost1 > cost2;
        } else {
            return cost1 < cost2;
        }
    }

    private SpanningTree combineApproach(SpanningTree tree) {
        /*
         * these are quick and fast techniques of returning a k-Tree without having to worry about not
         * ending up with a tree of k nodes *1, which was the main reason why the existing algorithm did not work.
         *
         * Teh first approach just cuts leaves from the final MST
         * The second approach grows the MST  step-by-step and cuts leaves when it needs to trim an edge
         *
         * Neither of this approach is the best by itself (the approach of cutting arbitrary heavy nodes is also
         * plagued by a lot of mistakes)
         * but it is not hard to construct situations where one works well and the other does not.
         * So I thought of combining the two get the best.
         *
         * I think we can have this ready for a 2.3.1 patch-release  (let's not rush it for thursday; it's alpha after all)
         * and for 2.4 we can try do write some more sophisticated methods (to get the optimal answer for a given
         * tree you need O(nk^2) time i think so it's maybe not doable*2)
         *
         * *1: we can modify the LinkCutTree so that we can check size of resulting sub trees after a cut, but for that
         * need to relearn the code :)
         * *2: and that is not guaranteed to be the optimal answer in general just for that particular tree :)
         *
         */
        var spanningTree1 = cutLeafApproach(tree); //should clone 'tree'
        var spanningTree2 = growApproach(tree);

        //TODO: Update totalWeight in the two methods
        if (spanningTree1.totalWeight() > spanningTree2.totalWeight()) {
            return (minMax == Prim.MAX_OPERATOR) ? spanningTree1 : spanningTree2;
        } else {
            return (minMax == Prim.MAX_OPERATOR) ? spanningTree2 : spanningTree1;

        }

    }
}


