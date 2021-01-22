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
package org.neo4j.graphalgo.beta.pregel.context;

import org.neo4j.graphalgo.beta.pregel.ComputeStep;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;

import java.util.stream.LongStream;

public abstract class NodeCentricContext<CONFIG extends PregelConfig> extends PregelContext<CONFIG> {

    protected final ComputeStep<CONFIG, ?> computeStep;

    long nodeId;

    NodeCentricContext(ComputeStep<CONFIG, ?> computeStep, CONFIG config) {
        super(config);
        this.computeStep = computeStep;
    }

    @Override
    public boolean isMultiGraph() {
        return computeStep.isMultiGraph();
    }

    @Override
    public long nodeCount() {
        return computeStep.nodeCount();
    }

    @Override
    public long relationshipCount() {
        return computeStep.relationshipCount();
    }

    /**
     * Used internally by the framework to set the currently processed node.
     */
    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * The identifier of the node that is currently processed.
     */
    public long nodeId() {
        return nodeId;
    }

    /**
     * Sets a node double value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, double value) {
        computeStep.setNodeValue(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, long value) {
        computeStep.setNodeValue(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, long[] value) {
        computeStep.setNodeValue(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, double[] value) {
        computeStep.setNodeValue(key, nodeId, value);
    }

    /**
     * Returns the degree (number of relationships) of the currently processed node.
     */
    public int degree() {
        return computeStep.degree(nodeId);
    }

    /**
     * Returns the outgoing neighbour-ids of the currently processed node.
     */
    public LongStream getNeighbours() { return computeStep.getNeighbors(nodeId); }

    public LongStream getNeighbours(long nodeId) { return computeStep.getNeighbors(nodeId); }
}


