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
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
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
        return combineApproach(spanningTree);
    }

    @NotNull
    private HugeLongPriorityQueue createPriorityQueue(long parentSize, boolean pruning) {
        boolean minQueue = minMax == Prim.MIN_OPERATOR;
        //if pruning, we remove the worst (max if it's a minimization problem)
        //therefore we flip the priority queue
        if (pruning) {
            minQueue = !minQueue;
        }
        HugeLongPriorityQueue priorityQueue = minQueue
            ? HugeLongPriorityQueue.min(parentSize)
            : HugeLongPriorityQueue.max(parentSize);
        return priorityQueue;
    }

    @Override
    public void release() {
        graph = null;
    }

    private double init(HugeLongArray parent, HugeDoubleArray costToParent, SpanningTree spanningTree) {
        graph.forEachNode((nodeId) -> {
            parent.set(nodeId, spanningTree.parent(nodeId));
            costToParent.set(nodeId, spanningTree.costToParent(nodeId));
            return true;
        });
        return spanningTree.totalWeight();
    }

    private SpanningTree cutLeafApproach(SpanningTree spanningTree) {
        //this approach cuts a leaf at each step (remaining graph is always corrected)
        //so we can just cut the most expensive leaf at each step
        var priorityQueue = createPriorityQueue(graph.nodeCount(), true);
        HugeLongArray degree = HugeLongArray.newArray(graph.nodeCount());
        long root = startNodeId;
        double rootCost = -1.0;
        long rootChild = -1;

        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray costToParent = HugeDoubleArray.newArray(graph.nodeCount());

        double totalCost = init(parent, costToParent, spanningTree);

        //calculate degree of each node in MST
        for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
            var nodeParent = parent.get(nodeId);
            if (nodeParent != -1) {
                degree.set(nodeParent, degree.get(nodeParent) + 1);
                degree.set(nodeId, degree.get(nodeId) + 1);

                if (nodeParent == root) { //root nodes needs special care because parent is -1
                    rootChild = nodeId;
                    rootCost = costToParent.get(nodeId);
                }
            }
        }
        //add all leafs in priority queue
        for (long nodeId = 0; nodeId < graph.nodeCount(); ++nodeId) {
            if (degree.get(nodeId) == 1) {
                double relevantCost = (nodeId == root) ?
                    rootCost :
                    costToParent.get(nodeId);
                priorityQueue.add(nodeId, relevantCost);
            }
        }

        long numberOfDeletions = spanningTree.effectiveNodeCount() - k;
        for (long i = 0; i < numberOfDeletions; ++i) {
            var nextNode = priorityQueue.pop();
            long affectedNode;
            
            if (nextNode == root) {
                affectedNode = rootChild;
                totalCost -= rootCost;
                clearNode(rootChild, parent, costToParent);
                root = affectedNode;
            } else {
                affectedNode = parent.get(nextNode);
                totalCost -= costToParent.get(nextNode);
                clearNode(nextNode, parent, costToParent);
            }

            degree.set(affectedNode, degree.get(affectedNode) - 1);
            double associatedCost = -1;
            if (degree.get(affectedNode) == 1) {
                if (affectedNode == root) {
                    //if it is root, we loop at its neighbors to find its single alive child
                    MutableDouble mutRootCost = new MutableDouble();
                    MutableLong mutRootChild = new MutableLong();
                    graph.forEachRelationship(root, (s, t) -> {
                        if (parent.get(t) == s) {
                            mutRootChild.setValue(t);
                            mutRootCost.setValue(costToParent.get(t));
                            return false;
                        }
                        return true;
                    });
                    rootChild = mutRootChild.longValue();
                    rootCost = mutRootCost.doubleValue();
                    associatedCost = rootCost;
                } else {
                    //otherwise we just get the info from parent
                    associatedCost = costToParent.get(affectedNode);
                }
                priorityQueue.add(affectedNode, associatedCost);

            }
        }
        return new SpanningTree(-1, graph.nodeCount(), k, parent, costToParent, totalCost);
    }

    private SpanningTree growApproach(SpanningTree spanningTree) {

        //this approach grows gradually the MST found in the previous step
        //when it is about to get larger than K, we crop the current worst leaf if the new value to be added
        // is actually better

        //TODO: Handle to be able to delete startNode as well (not much different from above approach)

        HugeLongArray outDegree = HugeLongArray.newArray(graph.nodeCount());

        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray costToParent = HugeDoubleArray.newArray(graph.nodeCount());

        init(parent, costToParent, spanningTree);
        double totalCost = 0;
        var priorityQueue = createPriorityQueue(graph.nodeCount(), false);
        var toTrim = createPriorityQueue(graph.nodeCount(), true);

        //priority-queue does not have a remove method so we need something to know if a node is still a leaf or not
        BitSet exterior = new BitSet(graph.nodeCount());
        //at any point, the tree has a root we mark its neighbors in this bitset to avoid looping to find them
        BitSet rootNodeAdjacent = new BitSet(graph.nodeCount());
        //we just save which nodes are in the final output and not (just to do clean-up; probably be avoided)
        BitSet included = new BitSet(graph.nodeCount());

        priorityQueue.add(startNodeId, 0);
        long root = startNodeId; //current root is startNodeId
        long nodesInTree = 0;
        while (!priorityQueue.isEmpty()) {
            long node = priorityQueue.top();
            double associatedCost = priorityQueue.cost(node);
            priorityQueue.pop();
            long nodeParent = parent.get(node);

            boolean nodeAdded = false;
            if (nodesInTree < k) { //if we are smaller, we can just add it no problemo
                nodesInTree++;
                nodeAdded = true;
            } else {
                while (!exterior.get(toTrim.top())) { //not valid frontier nodes anymore, just ignore
                    toTrim.pop(); //as we said, pq does not have a direct remove method
                }
                var nodeToTrim = toTrim.top(); //a leaf node with worst cost
                if (parent.get(node) == nodeToTrim) {
                    //we cannot add it, if we're supposed to remove its parent
                    //TODO: should be totally feasible to consider the 2nd worst then.
                    continue;
                }

                boolean shouldMove = moveMakesSense(associatedCost, toTrim.cost(nodeToTrim), minMax);

                if (shouldMove) {
                    nodeAdded = true;

                    double value = toTrim.cost(nodeToTrim);
                    toTrim.pop();

                    long parentOfTrimmed = parent.get(nodeToTrim);
                    included.clear(nodeToTrim); //nodeToTrim is removed from the answer
                    clearNode(nodeToTrim, parent, costToParent);
                    totalCost -= value; //as well as its cost from the solution

                    if (parentOfTrimmed != -1) { //we are not removing the actual root
                        //reduce degree of parent
                        outDegree.set(parentOfTrimmed, outDegree.get(parentOfTrimmed) - 1);
                        long affectedNode = -1;
                        double affectedCost = -1;
                        long parentDegree = outDegree.get(parentOfTrimmed);
                        if (parentOfTrimmed == root) {
                            rootNodeAdjacent.clear(nodeToTrim);
                            if (parentDegree == 1) { //it is a leaf
                                assert rootNodeAdjacent.cardinality() == 1;
                                var rootChild = rootNodeAdjacent.nextSetBit(0);
                                affectedNode = root;
                                affectedCost = costToParent.get(rootChild);
                            }
                        } else {
                            if (parentDegree == 0) {
                                affectedNode = parentOfTrimmed;
                                affectedCost = costToParent.get(parentOfTrimmed);
                            }
                        }
                        if (affectedNode != -1) {
                            toTrim.add(affectedNode, affectedCost);
                            exterior.set(affectedNode);
                        }
                    } else {
                        //the root is removed, long live the new root!
                        assert rootNodeAdjacent.cardinality() == 1;
                        var newRoot = rootNodeAdjacent.nextSetBit(0);
                        rootNodeAdjacent.clear(); //empty everything
                        graph.forEachRelationship(newRoot, (s, t) -> {
                            if (parent.get(t) == s) {
                                rootNodeAdjacent.set(t);
                            }
                            return true;
                        });
                        root = newRoot;
                        clearNode(root, parent, costToParent);
                        //see if root is a degree-1 to add to exterior
                        if (outDegree.get(root) == 1) {
                            var rootChild = rootNodeAdjacent.nextSetBit(0);
                            priorityQueue.add(rootChild, costToParent.get(rootChild));
                            exterior.set(root);
                        }
                    }
                }
            }
            if (nodeAdded) {
                included.set(node);
                totalCost += associatedCost;
                if (nodeParent == root) {
                    rootNodeAdjacent.set(node);
                }
                if (node != root) {
                    outDegree.set(nodeParent, outDegree.get(nodeParent) + 1);
                    exterior.clear(nodeParent);
                }
                toTrim.add(node, associatedCost);
                exterior.set(node);

                graph.forEachRelationship(node, 1.0, (s, t, w) -> {
                    if (parent.get(t) == s) {
                        //TODO: work's only on mst edges for now (should be doable to re-find an k-MST from whole graph)
                        if (!priorityQueue.containsElement(t)) {
                            priorityQueue.add(t, spanningTree.costToParent(t));
                        }

                    }
                    return true;
                });
            } else {
                clearNode(node, parent, costToParent);

            }
        }
        //post-processing step: anything not touched is reset to -1
        graph.forEachNode(nodeId -> {
            if (!included.get(nodeId)) {
                clearNode(nodeId, parent, costToParent);
            }
            return true;
        });

        return new SpanningTree(-1, graph.nodeCount(), k, parent, costToParent, totalCost);

    }

    private void clearNode(long node, HugeLongArray parent, HugeDoubleArray costToParent) {
        parent.set(node, -1);
        costToParent.set(node, -1);
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
         * Neither of this approach is the best by itself
         * (the approach of cutting the heaviest also has its shares of issues ofc)
         *
         * but it is not hard to construct situations where one works well and the other does not.
         * So I thought of combining the two and return the best
         *
         * This is supposed to be a quick fix to eliminate the bug asap. When we work next time on k-MST,
         * we can come up with something more sophisticated (there's optimal algorithms for dealing with subtree;
         * but they take O(nk^2) so maybe not good). Otherwise, it's np-complete so...
         */
        var spanningTree1 = cutLeafApproach(tree);
        var spanningTree2 = growApproach(tree);
        System.out.println(spanningTree1.totalWeight() + " " + spanningTree2.totalWeight());

        if (spanningTree1.totalWeight() > spanningTree2.totalWeight()) {
            return (minMax == Prim.MAX_OPERATOR) ? spanningTree1 : spanningTree2;
        } else {
            return (minMax == Prim.MAX_OPERATOR) ? spanningTree2 : spanningTree1;

        }

    }
}


