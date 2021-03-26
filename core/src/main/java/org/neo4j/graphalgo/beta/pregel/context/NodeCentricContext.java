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

import java.util.function.LongConsumer;

public abstract class NodeCentricContext<CONFIG extends PregelConfig> extends PregelContext<CONFIG> {

    protected final ComputeStep computeStep;

    long nodeId;

    NodeCentricContext(ComputeStep computeStep, CONFIG config) {
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
     * Calls the consumer for each neighbor of the currently processed node.
     */
    public void forEachNeighbor(LongConsumer targetConsumer) {
        computeStep.forEachNeighbor(nodeId, targetConsumer);
    }

    /**
     * Calls the consumer for each neighbor of the given node.
     */
    public void forEachNeighbor(long nodeId, LongConsumer targetConsumer) {
        computeStep.forEachNeighbor(nodeId, targetConsumer);
    }

    /**
     * Calls the consumer once for each neighbor of the currently processed node.
     */
    public void forEachDistinctNeighbor(LongConsumer targetConsumer) {
        computeStep.forEachDistinctNeighbor(nodeId, targetConsumer);
    }

    /**
     * Calls the consumer once for each neighbor of the given node.
     */
    public void forEachDistinctNeighbor(long nodeId, LongConsumer targetConsumer) {
        computeStep.forEachDistinctNeighbor(nodeId, targetConsumer);
    }
}


