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
package org.neo4j.gds.scc;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.PagedLongStack;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

/**
 * huge iterative (non recursive) sequential strongly connected components algorithm.
 *
 * specified in:  http://code.activestate.com/recipes/578507-strongly-connected-components-of-a-directed-graph/
 */
public class Scc extends Algorithm<HugeLongArray> {
    public static final int UNORDERED = -1;
    public static final String SCC_DESCRIPTION = "The SCC algorithm finds sets of connected nodes in an directed graph, " +
                                                 "where all nodes in the same set form a connected component.";
    private final Graph graph;
    private final HugeLongArrayStack boundaries;
    private final HugeLongArray connectedComponents;
    private final HugeLongArray index;
    private final HugeLongArrayStack stack;
    private final PagedLongStack todo; // stores nodeIds either positive (edge visit) or negative (node visit)
    private final BitSet visited;

    public Scc(
        Graph graph,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);

        this.graph = graph;
        var nodeCount = this.graph.nodeCount();

        this.boundaries = HugeLongArrayStack.newStack(nodeCount);
        this.connectedComponents = HugeLongArray.newArray(nodeCount);
        this.index = HugeLongArray.newArray(nodeCount);
        this.stack = HugeLongArrayStack.newStack(nodeCount);
        this.todo = new PagedLongStack(nodeCount); //can be as high as `2 * graph.relationshipsCount()` if we are unlucky...
        this.visited = new BitSet(nodeCount);
    }

    /**
     * compute scc
     */
    public HugeLongArray compute() {
        progressTracker.beginSubTask();
        index.fill(UNORDERED);
        connectedComponents.fill(UNORDERED);

        graph.forEachNode(this::computePerNode); //this will visit 0 first
        progressTracker.endSubTask();
        return connectedComponents;
    }

    private boolean computePerNode(long nodeId) {
        if (!terminationFlag.running()) {
            return false;
        }

        if (index.get(nodeId) != UNORDERED) {
            return true;
        }

        todo.clear();

        if (nodeId == 0) {
            return computeForZero(); //specially handle 0 because -0 = 0;
        }

        todo.push(-nodeId);

        while (!todo.isEmpty()) {
            var node = todo.pop();

            if (node < 0) { // if the node is <0, we know we are going to visit a node as a node

                var actualNode = -node;

                if (index.get(node) == UNORDERED) { //if the node has not been examined before, it is it's first visit
                    visitNode(actualNode);
                } else { //otherwise it is its last
                    postVisitNode(actualNode);
                }

            } else { //otherwise it 's an  visit edge
                visitEdge(node);
            }
        }
        progressTracker.logProgress();
        return true;
    }


    private boolean computeForZero() {

        visitNode(0);//we know the first action:   we visit node 0
        //visit node pushes  0 at the stack and it will remain until the end there
        while (!todo.isEmpty()) {
            var node = todo.pop();

            if (node < 0) { // if the node is <0, we know we are going to visit a node as a node

                var actualNode = -node;

                if (index.get(node) == UNORDERED) { //if the node has not been examined before, it is it's first visit
                    visitNode(actualNode);
                } else { //otherwise it is its last
                    postVisitNode(actualNode);
                }

            } else if (node > 0) { //otherwise it 's an edge
                visitEdge(node);
            } else { //the zero case
                //-0 = 0 , so a 0 can indicate two things either a visit edge to 0, or the post visit to 0 which will conclude this loop
                //so depending on whether stack is empty or not we can distinguish between the two cases
                if (todo.isEmpty()) {
                    postVisitNode(0);
                } else {
                    visitEdge(0);
                }
            }
        }
        progressTracker.logProgress();
        return true;
    }


    private void visitNode(long nodeId) {
        final long stackSize = stack.size();
        index.set(nodeId, stackSize);
        stack.push(nodeId); // push to stack (at most one entry per vertex)
        boundaries.push(stackSize); // push to stack (at most one entry per vertex)
        todo.push(-nodeId);
        graph.forEachRelationship(nodeId, (s, t) -> {
            todo.push(t);
            return true;
        });
    }

    private void visitEdge(long nodeId) {
        if (index.get(nodeId) == UNORDERED) {
            todo.push(-nodeId); //organize a first visit to nodeId
        } else if (!visited.get(nodeId)) {          //skip nodes already in a component
            while (index.get(nodeId) < boundaries.peek()) {
                boundaries.pop();
            }
        }
    }

    private void postVisitNode(long nodeId) {
        if (boundaries.peek() == index.get(nodeId)) {
            boundaries.pop();
            long element;
            do {
                element = stack.pop(); //pop to stack
                connectedComponents.set(element, nodeId);
                visited.set(element);
            } while (element != nodeId);
        }
    }


}
