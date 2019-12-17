/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.scc;

import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.PagedLongStack;
import org.neo4j.graphalgo.core.utils.paged.PagedSimpleBitSet;
import org.neo4j.graphdb.Direction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * huge iterative (non recursive) sequential strongly connected components algorithm.
 *
 * specified in:  http://code.activestate.com/recipes/578507-strongly-connected-components-of-a-directed-graph/
 */
public class SCCAlgorithm extends LegacyAlgorithm<SCCAlgorithm> {

    /**
     * returns a initialized SCC algorithm based on which
     * type of graph has been supplied.
     *
     * @param graph
     * @param tracker
     * @return
     */
    public static SCCAlgorithm iterativeTarjan(Graph graph, AllocationTracker tracker) {
        return new SCCAlgorithm(graph, tracker);
    }

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
    private PagedSimpleBitSet visited;
    private HugeLongArray connectedComponents;
    private PagedLongStack stack;
    private PagedLongStack boundaries;
    private PagedLongStack todo; // stores pairs of (node-Id, TODO-Id)
    private int setCount;

    private int minSetSize;
    private int maxSetSize;

    public SCCAlgorithm(Graph graph, AllocationTracker tracker) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        index = HugeLongArray.newArray(nodeCount, tracker);
        stack = new PagedLongStack(nodeCount, tracker);
        boundaries = new PagedLongStack(nodeCount, tracker);
        connectedComponents = HugeLongArray.newArray(nodeCount, tracker);
        visited = PagedSimpleBitSet.newBitSet(nodeCount, tracker);
        todo = new PagedLongStack(nodeCount, tracker);
    }

    /**
     * compute scc
     * @return
     */
    public Void compute() {
        setCount = 0;
        minSetSize = Integer.MAX_VALUE;
        maxSetSize = 0;
        index.fill(-1);
        connectedComponents.fill(-1);
        todo.clear();
        boundaries.clear();
        stack.clear();
        graph.forEachNode(this::compute);
        return null;
    }

    @Override
    public SCCAlgorithm me() {
        return this;
    }

    /**
     * release inner data structures
     */
    @Override
    public void release() {
        graph = null;
        index = null;
        visited = null;
        connectedComponents = null;
        stack = null;
        boundaries = null;
        todo = null;
    }

    /**
     * get nodeId to component id mapping
     * @return
     */
    public HugeLongArray getConnectedComponents() {
        return connectedComponents;
    }

    /**
     * get stream of original nodeId to component id pairs
     * @return
     */
    public Stream<SCCAlgorithm.StreamResult> resultStream() {
        return LongStream.range(0, nodeCount)
                .filter(i -> connectedComponents.get(i) != -1)
                .mapToObj(i -> new SCCAlgorithm.StreamResult(graph.toOriginalNodeId(i), connectedComponents.get(i)));
    }

    /**
     * number of connected components in the graph
     * @return
     */
    public long getSetCount() {
        return setCount;
    }

    /**
     * minimum set size
     * @return
     */
    public long getMinSetSize() {
        return minSetSize;
    }

    /**
     * maximum component size
     * @return
     */
    public long getMaxSetSize() {
        return maxSetSize;
    }

    private boolean compute(long nodeId) {
        if (!running()) {
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
        getProgressLogger().logProgress((double) nodeId / (nodeCount - 1));
        return true;
    }

    private void visitEdge(long nodeId) {
        if (index.get(nodeId) == -1) {
            push(Action.VISIT, nodeId);
        } else if (!visited.contains(nodeId)) {
            while (index.get(nodeId) < boundaries.peek()) {
                boundaries.pop();
            }
        }
    }

    private void postVisit(long nodeId) {
        if (boundaries.peek() == index.get(nodeId)) {
            boundaries.pop();
            int elementCount = 0;
            long element;
            do {
                element = stack.pop();
                connectedComponents.set(element, nodeId);
                visited.put(element);
                elementCount++;
            } while (element != nodeId);
            minSetSize = Math.min(minSetSize, elementCount);
            maxSetSize = Math.max(maxSetSize, elementCount);
            setCount++;
        }

    }

    private void visit(long nodeId) {
        final long stackSize = stack.size();
        index.set(nodeId, stackSize);
        stack.push(nodeId);
        boundaries.push(stackSize);
        push(Action.POSTVISIT, nodeId);
        graph.forEachRelationship(nodeId, Direction.OUTGOING, (s, t) -> {
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

    /**
     * stream result type
     */
    public static class StreamResult {

        public final long nodeId;
        public final long partition;

        public StreamResult(long nodeId, long partition) {
            this.nodeId = nodeId;
            this.partition = partition;
        }
    }
}
