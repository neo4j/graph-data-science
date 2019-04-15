/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.results.SCCStreamResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.collection.SimpleBitSet;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * non recursive sequential strongly connected components algorithm.
 * <p>
 * taken from: http://code.activestate.com/recipes/578507-strongly-connected-components-of-a-directed-graph/
 */
public class SCCIterativeTarjan extends Algorithm<SCCIterativeTarjan> implements SCCAlgorithm {

    private enum Action {
        VISIT(0),
        VISITEDGE(1),
        POSTVISIT(2);

        public final int code;

        Action(int code) {
            this.code = code;
        }

    }

    private Graph graph;

    private final int nodeCount;
    private int[] index;
    private SimpleBitSet visited;
    private int[] connectedComponents;
    private IntStack stack;
    private IntStack boundaries;
    // the stack is used to keep track of pairs of {node, action}
    private IntStack todo;
    // number of strongly connected components
    private int setCount;
    private int minSetSize;
    private int maxSetSize;

    public SCCIterativeTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        index = new int[nodeCount];
        stack = new IntStack();
        boundaries = new IntStack();
        connectedComponents = new int[nodeCount];
        visited = new SimpleBitSet(nodeCount);
        todo = new IntStack();
    }

    public SCCIterativeTarjan compute() {
        setCount = 0;
        minSetSize = Integer.MAX_VALUE;
        maxSetSize = 0;
        Arrays.fill(index, -1);
        Arrays.fill(connectedComponents, -1);
        todo.clear();
        boundaries.clear();
        stack.clear();
        graph.forEachNode(this::compute);
        return this;
    }

    @Override
    public SCCIterativeTarjan me() {
        return this;
    }

    @Override
    public SCCIterativeTarjan release() {
        graph = null;
        index = null;
        visited = null;
        connectedComponents = null;
        stack = null;
        boundaries = null;
        todo = null;
        return this;
    }

    public int[] getConnectedComponents() {
        return connectedComponents;
    }

    public Stream<SCCAlgorithm.StreamResult> resultStream() {
        return IntStream.range(0, nodeCount)
                .filter(i -> connectedComponents[i] != -1)
                .mapToObj(i -> new SCCAlgorithm.StreamResult(graph.toOriginalNodeId(i), connectedComponents[i]));
    }

    public long getSetCount() {
        return setCount;
    }

    public long getMinSetSize() {
        return minSetSize;
    }

    public long getMaxSetSize() {
        return maxSetSize;
    }

    private boolean compute(int nodeId) {
        if (!running()) {
            return false;
        }
        if (index[nodeId] != -1) {
            return true;
        }
        push(Action.VISIT, nodeId);
        while (!todo.isEmpty()) {
            final int action = todo.pop();
            final int node = todo.pop();
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

    private void visitEdge(int nodeId) {
        if (index[nodeId] == -1) {
            push(Action.VISIT, nodeId);
        } else if (!visited.contains(nodeId)) {
            while (index[nodeId] < boundaries.peek()) {
                boundaries.pop();
            }
        }
    }

    private void postVisit(int nodeId) {
        if (boundaries.peek() == index[nodeId]) {
            boundaries.pop();
            int elementCount = 0;
            int element;
            do {
                element = stack.pop();
                connectedComponents[element] = nodeId;
                visited.put(element);
                elementCount++;
            } while (element != nodeId);
            minSetSize = Math.min(minSetSize, elementCount);
            maxSetSize = Math.max(maxSetSize, elementCount);
            setCount++;
        }

    }

    private void visit(int nodeId) {
        final int stackSize = stack.size();
        index[nodeId] = stackSize;
        stack.push(nodeId);
        boundaries.push(stackSize);
        push(Action.POSTVISIT, nodeId);
        graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId) -> {
            push(Action.VISITEDGE, targetNodeId);
            return true;
        });
    }

    /**
     * pushes an action and a nodeId on the stack
     *
     * @param action
     * @param value
     */
    private void push(Action action, int value) {
        todo.push(value);
        todo.push(action.code);
    }
}
