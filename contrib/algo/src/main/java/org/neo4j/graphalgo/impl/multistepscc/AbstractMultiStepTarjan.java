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
package org.neo4j.graphalgo.impl.multistepscc;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

/**
 * Abstract impl. of Tarjan Strongly Connected Components
 *
 * @author mknblch
 */
public abstract class AbstractMultiStepTarjan {

    private final Graph graph;
    // determine if the node is on the stack
    private BitSet onStack;
    // index map
    private IntIntMap indices;
    // low link map
    private IntIntMap lowLink;
    // working stack
    private IntStack stack;
    // current index
    private int index;

    public AbstractMultiStepTarjan(Graph graph) {
        this.graph = graph;
    }

    public AbstractMultiStepTarjan compute(IntSet nodes) {

        indices = new IntIntScatterMap(nodes.size());
        lowLink = new IntIntScatterMap(nodes.size());
        stack = new IntStack(nodes.size());
        onStack = new BitSet();
        nodes.forEach((IntProcedure) this::strongConnect);
        return this;
    }

    private void reset() {
        indices.clear();
        lowLink.clear();
        onStack.clear();
        stack.clear();
        index = 0;
    }

    private void strongConnect(int node) {
        if (indices.containsKey(node)) {
            return;
        }
        lowLink.put(node, index);
        indices.put(node, index);
        index++;
        stack.push(node);
        onStack.set(node);
        graph.forEachRelationship(node, Direction.OUTGOING, this::accept);
        if (indices.get(node) == lowLink.get(node)) {
            relax(node);
        }
    }

    private void relax(int nodeId) {
        IntHashSet connected = new IntHashSet();
        int w;
        do {
            w = stack.pop();
            onStack.clear(w);
            connected.add(w);
        } while (w != nodeId);
        processSCC(nodeId, connected);
    }

    private boolean accept(int source, int target, long edgeId) {
        if (!indices.containsKey(target)) {
            strongConnect(target);
            lowLink.put(source, Math.min(lowLink.get(source), lowLink.get(target)));
        } else if (onStack.get(target)) {
            lowLink.put(source, Math.min(lowLink.get(source), indices.get(target)));
        }
        return true;
    }

    public abstract void processSCC(int root, IntHashSet connected);
}
