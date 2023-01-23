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

        var outputTree = growApproach(spanningTree);
        progressTracker.endSubTask();
        return outputTree;
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
    
    private double init(HugeLongArray parent, HugeDoubleArray costToParent, SpanningTree spanningTree) {
        graph.forEachNode((nodeId) -> {
            parent.set(nodeId, spanningTree.parent(nodeId));
            costToParent.set(nodeId, spanningTree.costToParent(nodeId));
            return true;
        });
        return spanningTree.totalWeight();
    }


    private SpanningTree growApproach(SpanningTree spanningTree) {

        //this approach grows gradually the MST found in the previous step
        //when it is about to get larger than K, we crop the current worst leaf if the new value to be added
        // is actually better
        if (spanningTree.effectiveNodeCount() < k)
            return spanningTree;

        HugeLongArray outDegree = HugeLongArray.newArray(graph.nodeCount());

        HugeLongArray parent = HugeLongArray.newArray(graph.nodeCount());
        HugeDoubleArray costToParent = HugeDoubleArray.newArray(graph.nodeCount());

        init(parent, costToParent, spanningTree);
        double totalCost = 0;
        var priorityQueue = createPriorityQueue(graph.nodeCount(), false);
        var toTrim = createPriorityQueue(graph.nodeCount(), true);

        //priority-queue does not have a remove method
        // so we need something to know if a node is still a leaf or not
        BitSet exterior = new BitSet(graph.nodeCount());
        //at any point, the tree has a root we mark its neighbors in this bitset to avoid looping to find them
        BitSet rootNodeAdjacent = new BitSet(graph.nodeCount());
        //we just save which nodes are in the final output and not (just to do clean-up; probably can be avoided)
        BitSet included = new BitSet(graph.nodeCount());

        priorityQueue.add(startNodeId, 0);
        long root = startNodeId; //current root is startNodeId
        long nodesInTree = 0;
        progressTracker.beginSubTask(graph.nodeCount());
        while (!priorityQueue.isEmpty()) {
            long node = priorityQueue.top();
            progressTracker.logProgress();
            double associatedCost = priorityQueue.cost(node);
            priorityQueue.pop();
            long nodeParent = parent.get(node);

            boolean nodeAdded = false;
            if (nodesInTree < k) { //if we are smaller, we can just add it no problemo
                nodesInTree++;
                nodeAdded = true;
            } else {
                var nodeToTrim = findNextValidLeaf(toTrim, exterior); //a leaf node with currently theworst cost
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

                    if (root != nodeToTrim) { //we are not removing the actual root
                        //reduce degree of parent
                        outDegree.set(parentOfTrimmed, outDegree.get(parentOfTrimmed) - 1);
                        long affectedNode = -1;
                        double affectedCost = -1;
                        long parentOutDegree = outDegree.get(parentOfTrimmed);
                        if (parentOfTrimmed == root) { //if its parent is the root
                            rootNodeAdjacent.clear(nodeToTrim); //remove the trimmed child
                            if (parentOutDegree == 1) { //root becomes a leaf
                                assert rootNodeAdjacent.cardinality() == 1;
                                //get the single sole child of root
                                var rootChild = rootNodeAdjacent.nextSetBit(0);
                                affectedNode = root;
                                affectedCost = costToParent.get(rootChild);
                            }
                        } else {
                            if (parentOutDegree == 0) { //if parent becomes a leaf
                                affectedNode = parentOfTrimmed;
                                affectedCost = costToParent.get(parentOfTrimmed);
                            }
                        }
                        if (affectedNode != -1) { //if a node has been converted to a leaf
                            updateExterior(affectedNode, affectedCost, toTrim, exterior);
                        }
                    } else {
                        //the root is removed, long live the new root!
                        assert rootNodeAdjacent.cardinality() == 1;
                        //the new root is the single sole child of old root
                        var newRoot = rootNodeAdjacent.nextSetBit(0);
                        rootNodeAdjacent.clear(); //empty everything
                        //find the children of the new root (this can happen once per node)

                        fillChildren(newRoot, rootNodeAdjacent, parent, included);

                        root = newRoot;
                        //set it as root
                        clearNode(root, parent, costToParent);
                        //check if root is a degree-1 to add to exterior
                        if (outDegree.get(root) == 1) {
                            //get single child
                            var rootChild = rootNodeAdjacent.nextSetBit(0);
                            priorityQueue.add(root, costToParent.get(rootChild));
                            exterior.set(root);
                        }
                    }
                }
            }
            if (nodeAdded) {
                included.set(node); // include it in the solution (for now!)
                totalCost += associatedCost; //add its associated cost to the weight of tree
                if (nodeParent == root) { //if it's parent is the root, update the bitset
                    rootNodeAdjacent.set(node);
                }
                if (node != root) { //this only happens for startNode to be fair
                    //the node's parent gets an update in degree
                    outDegree.set(nodeParent, outDegree.get(nodeParent) + 1);
                    exterior.clear(nodeParent); //and remoed from exterior if included
                }
                //then the node  (being a leaf) is added to the trimming priority queu
                toTrim.add(node, associatedCost);
                exterior.set(node); //and the exterior
                relaxNode(node, priorityQueue, parent, spanningTree);

            } else {
                clearNode(node, parent, costToParent);
            }
        }
        //post-processing step: anything not touched is reset to -1
        pruneUntouchedNodes(parent, costToParent, included);
        progressTracker.endSubTask();
        return new SpanningTree(root, graph.nodeCount(), k, parent, costToParent, totalCost);

    }

    private void pruneUntouchedNodes(HugeLongArray parent, HugeDoubleArray costToParent, BitSet included) {
        graph.forEachNode(nodeId -> {
            if (!included.get(nodeId)) {
                clearNode(nodeId, parent, costToParent);
            }
            return true;
        });
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

    private void updateExterior(long affectedNode, double affectedCost, HugeLongPriorityQueue toTrim, BitSet exterior) {
        if (!toTrim.containsElement(affectedNode)) {
            toTrim.add(affectedNode, affectedCost); //add it to pq
        } else {
            //it is still in the queue, but it is not a leaf anymore, so it's value is obsolete
            toTrim.set(affectedNode, affectedCost);
        }
        exterior.set(affectedNode); //and mark it in the exterior
    }

    private long findNextValidLeaf(HugeLongPriorityQueue toTrim, BitSet exterior) {
        while (!exterior.get(toTrim.top())) { //not valid frontier nodes anymore, just ignore
            toTrim.pop(); //as we said, pq does not have a direct remove method
        }
        return toTrim.top();
    }

    private void fillChildren(long newRoot, BitSet rootNodeAdjacent, HugeLongArray parent, BitSet included) {
        graph.forEachRelationship(newRoot, (s, t) -> {
            //relevant are only those nodes which are currently
            //in the k-tree
            if (parent.get(t) == s && included.get(t)) {
                rootNodeAdjacent.set(t);
            }
            return true;
        });
    }

    private void relaxNode(
        long node,
        HugeLongPriorityQueue priorityQueue,
        HugeLongArray parent,
        SpanningTree spanningTree
    ) {
        graph.forEachRelationship(node, (s, t) -> {
            if (parent.get(t) == s) {
                //TODO: work's only on mst edges for now (should be doable to re-find an k-MST from whole graph)
                if (!priorityQueue.containsElement(t)) {
                    priorityQueue.add(t, spanningTree.costToParent(t));
                }

            }
            return true;
        });
    }

}


