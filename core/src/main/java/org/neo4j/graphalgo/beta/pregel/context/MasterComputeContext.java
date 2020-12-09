/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel.context;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;

import java.util.function.LongPredicate;

public class MasterComputeContext<CONFIG extends PregelConfig> extends PregelContext<CONFIG> {

    private final Graph graph;
    private final int iteration;
    private final Pregel.CompositeNodeValue nodeValues;

    public MasterComputeContext(
        CONFIG config,
        Graph graph,
        int iteration,
        Pregel.CompositeNodeValue nodeValues
    ) {
        super(config);
        this.graph = graph;
        this.iteration = iteration;
        this.nodeValues = nodeValues;
    }

    @Override
    public boolean isMultiGraph() {
        return graph.isMultiGraph();
    }

    @Override
    public long nodeCount() {
        return graph.nodeCount();
    }

    @Override
    public long relationshipCount() {
        return graph.relationshipCount();
    }

    /**
     * Returns the current superstep (0-based).
     */
    public int superstep() {
        return iteration;
    }

    /**
     * Accepts a consumer function that is called for every node in the graph.
     * The consumer receives one node id at the time.
     * If the consumer returns true, the next node is passed in. Otherwise the iteration stops.
     * @param consumer
     */
    public void forEachNode(LongPredicate consumer) {
        graph.forEachNode(consumer);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a double
     */
    public double doubleNodeValue(long nodeId, String key) {
        return nodeValues.doubleValue(key, nodeId);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a long
     */
    public long longNodeValue(long nodeId, String key) {
        return nodeValues.longValue(key, nodeId);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a long array
     */
    public long[] longArrayNodeValue(long nodeId, String key) {
        return nodeValues.longArrayValue(key, nodeId);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a double array
     */
    public double[] doubleArrayNodeValue(long nodeId, String key) {
        return nodeValues.doubleArrayValue(key, nodeId);
    }

    /**
     * Sets a node double value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(long nodeId, String key, double value) {
        nodeValues.set(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(long nodeId, String key, long value) {
        nodeValues.set(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(long nodeId, String key, long[] value) {
        nodeValues.set(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(long nodeId, String key, double[] value) {
        nodeValues.set(key, nodeId, value);
    }
}
