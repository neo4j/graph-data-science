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

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.BitSet;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;
import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntPredicate;

/**
 * Sequential, recursive strongly connected components algorithm (Tarjan).
 *
 * https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
 *
 * Builds sets of node-Ids which represent a strongly connected component
 * within the graph. Also calculates minimum and maximum setSize as well
 * as the overall count of distinct sets.
 */
public class SCCTarjan extends LegacyAlgorithm<SCCTarjan> {

    private Graph graph;
    private final int nodeCount;
    private int[] communities;
    private int[] indices;
    private int[] lowLink;
    private final BitSet onStack;
    private final IntStack stack;
    private int index;


    public SCCTarjan(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        indices = new int[nodeCount];
        lowLink = new int[nodeCount];
        onStack = new BitSet(nodeCount);
        stack = new IntStack(nodeCount);
        communities = new int[nodeCount];
        Arrays.setAll(communities, i -> i);
    }

    /**
     * compute scc
     * @return
     */
    public Boolean compute() {
        graph.forEachNode(longToIntPredicate(this::test));
        return true;
    }

    /**
     * return nodeId to component-id mapping
     * @return
     */
    public int[] getConnectedComponents() {
        return communities;
    }

    @Override
    public SCCTarjan me() {
        return this;
    }

    @Override
    public void release() {
        stack.clear();
        communities = null;
        indices = null;
        lowLink = null;
        graph = null;
    }

    /**
     * reset algorithm state
     */
    public void reset() {
        Arrays.fill(indices, -1);
        Arrays.fill(lowLink, -1);
        onStack.clear();
        stack.clear();
        index = 0;
    }

    private void strongConnect(int node) {
        lowLink[node] = index;
        indices[node] = index++;
        stack.push(node);
        onStack.set(node);
        graph.concurrentCopy().forEachRelationship(node, Direction.OUTGOING, longToIntConsumer(this::accept));
        if (indices[node] == lowLink[node]) {
            relax(node);
        }
    }

    private void relax(int nodeId) {
        int w;
        do {
            w = stack.pop();
            onStack.clear(w);
            communities[w] = nodeId;
        } while (w != nodeId);
    }

    private boolean accept(int source, int target) {
        if (indices[target] == -1) {
            strongConnect(target);
            lowLink[source] = Math.min(lowLink[source], lowLink[target]);
        } else if (onStack.get(target)) {
            lowLink[source] = Math.min(lowLink[source], indices[target]);
        }
        return true;
    }

    private boolean test(int node) {
        if (indices[node] == -1) {
            strongConnect(node);
        }
        progressLogger.logProgress((double) node / (nodeCount - 1));
        return running();
    }
}
