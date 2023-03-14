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
package org.neo4j.gds.impl.scc;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.PagedLongStack;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

/**
 * huge iterative (non recursive) sequential strongly connected components algorithm.
 *
 * specified in:  http://code.activestate.com/recipes/578507-strongly-connected-components-of-a-directed-graph/
 */
public class Scc extends Algorithm<HugeLongArray> {

    private enum Action {
        VISIT(0L),
        VISITEDGE(1L),
        POSTVISIT(2L);

        public final long code;

        Action(long code) {
            this.code = code;
        }

    }

    private Graph graph;

    private final long nodeCount;
    private HugeLongArray index;
    private BitSet visited;
    private HugeLongArray connectedComponents;
    private PagedLongStack stack;
    private PagedLongStack boundaries;
    private PagedLongStack todo; // stores pairs of (node-Id, TODO-Id)

    public Scc(
        Graph graph,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.index = HugeLongArray.newArray(nodeCount);
        this.stack = new PagedLongStack(nodeCount);
        this.boundaries = new PagedLongStack(nodeCount);
        this.connectedComponents = HugeLongArray.newArray(nodeCount);
        this.visited = new BitSet(nodeCount);
        this.todo = new PagedLongStack(nodeCount);
    }

    /**
     * compute scc
     */
    public HugeLongArray compute() {
        progressTracker.beginSubTask();
        index.fill(-1);
        connectedComponents.fill(-1);
        todo.clear();
        boundaries.clear();
        stack.clear();
        graph.forEachNode(this::compute);
        progressTracker.endSubTask();
        return connectedComponents;
    }

    private boolean compute(long nodeId) {
        if (!terminationFlag.running()) {
            return false;
        }
        if (index.get(nodeId) != -1) {
            return true;
        }
        push(Action.VISIT, nodeId);
        while (!todo.isEmpty()) {
            final long action = todo.pop();
            final long node = todo.pop();
            if (action == Action.VISIT.code) {
                visit(node);
            } else if (action == Action.VISITEDGE.code) {
                visitEdge(node);
            } else {
                postVisit(node);
            }
        }
        progressTracker.logProgress();
        return true;
    }

    private void visitEdge(long nodeId) {
        if (index.get(nodeId) == -1) {
            push(Action.VISIT, nodeId);
        } else if (!visited.get(nodeId)) {
            while (index.get(nodeId) < boundaries.peek()) {
                boundaries.pop();
            }
        }
    }

    private void postVisit(long nodeId) {
        if (boundaries.peek() == index.get(nodeId)) {
            boundaries.pop();
            long element;
            do {
                element = stack.pop();
                connectedComponents.set(element, nodeId);
                visited.set(element);
            } while (element != nodeId);
        }

    }

    private void visit(long nodeId) {
        final long stackSize = stack.size();
        index.set(nodeId, stackSize);
        stack.push(nodeId);
        boundaries.push(stackSize);
        push(Action.POSTVISIT, nodeId);
        graph.forEachRelationship(nodeId, (s, t) -> {
            push(Action.VISITEDGE, t);
            return true;
        });
    }

    /**
     * pushes an action and a nodeId on the stack
     *
     * @param action
     * @param value
     */
    private void push(Action action, long value) {
        todo.push(value);
        todo.push(action.code);
    }

}
