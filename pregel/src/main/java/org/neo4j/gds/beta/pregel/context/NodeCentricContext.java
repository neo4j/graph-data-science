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
package org.neo4j.gds.beta.pregel.context;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.NodeValue;
import org.neo4j.gds.beta.pregel.PregelConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.function.LongConsumer;

public abstract class NodeCentricContext<CONFIG extends PregelConfig> extends PregelContext<CONFIG> {

    protected final NodeValue nodeValue;

    protected final Graph graph;

    long nodeId;

    NodeCentricContext(Graph graph, CONFIG config, NodeValue nodeValue, ProgressTracker progressTracker) {
        super(config, progressTracker);
        this.graph = graph;
        this.nodeValue = nodeValue;
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
        nodeValue.set(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, long value) {
        nodeValue.set(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, long[] value) {
        nodeValue.set(key, nodeId, value);
    }

    /**
     * Sets a node long value for given the node schema key.
     *
     * @param key node schema key
     * @param value property value
     */
    public void setNodeValue(String key, double[] value) {
        nodeValue.set(key, nodeId, value);
    }

    /**
     * Returns the degree (number of relationships) of the currently processed node.
     */
    public int degree() {
        return graph.degree(nodeId);
    }

    /**
     * Returns the corresponding node id in the original graph for the current node id.
     */
    public long toOriginalId() {
        return toOriginalId(nodeId());
    }

    /**
     * Returns the corresponding node id in the original graph for the given internal node id.
     *
     * @param internalNodeId a node id in the in-memory graph
     */
    public long toOriginalId(long internalNodeId) {
        return graph.toOriginalNodeId(internalNodeId);
    }

    /**
     * Returns the corresponding node id in the internal graph for the given original node id.
     *
     * @param originalNodeId a node id in the original graph
     */
    public long toInternalId(long originalNodeId) {
        return graph.toMappedNodeId(originalNodeId);
    }

    /**
     * Calls the consumer for each neighbor of the currently processed node.
     */
    public void forEachNeighbor(LongConsumer targetConsumer) {
        graph.forEachRelationship(nodeId, (ignored, targetNodeId) -> {
            targetConsumer.accept(targetNodeId);
            return true;
        });
    }

    /**
     * Calls the consumer for each neighbor of the given node.
     */
    public void forEachNeighbor(long nodeId, LongConsumer targetConsumer) {
        graph.forEachRelationship(nodeId, (ignored, targetNodeId) -> {
            targetConsumer.accept(targetNodeId);
            return true;
        });
    }

    /**
     * Calls the consumer once for each neighbor of the currently processed node.
     */
    public void forEachDistinctNeighbor(LongConsumer targetConsumer) {
        forEachDistinctNeighbor(nodeId, targetConsumer);
    }

    /**
     * Calls the consumer once for each neighbor of the given node.
     */
    public void forEachDistinctNeighbor(long nodeId, LongConsumer targetConsumer) {
        var prevTarget = new MutableLong(-1);
        graph.forEachRelationship(nodeId, (ignored, targetNodeId) -> {
            if (prevTarget.longValue() != targetNodeId) {
                targetConsumer.accept(targetNodeId);
                prevTarget.setValue(targetNodeId);
            }
            return true;
        });
    }

    interface BidirectionalNodeCentricContext {
        Graph graph();

        long nodeId();

        /**
         * Returns the incoming degree (number of relationships) of the currently processed node.
         */
        default int incomingDegree() {
            return graph().degreeInverse(nodeId());
        }

        /**
         * Calls the consumer for each incoming neighbor of the currently processed node.
         */
        default void forEachIncomingNeighbor(LongConsumer targetConsumer) {
            graph().forEachInverseRelationship(nodeId(), (ignored, targetNodeId) -> {
                targetConsumer.accept(targetNodeId);
                return true;
            });
        }

        /**
         * Calls the consumer for each incoming neighbor of the given node.
         */
        default void forEachIncomingNeighbor(long nodeId, LongConsumer targetConsumer) {
            graph().forEachInverseRelationship(nodeId, (ignored, targetNodeId) -> {
                targetConsumer.accept(targetNodeId);
                return true;
            });
        }

        /**
         * Calls the consumer once for each incoming neighbor of the currently processed node.
         */
        default void forEachDistinctIncomingNeighbor(LongConsumer targetConsumer) {
            forEachDistinctIncomingNeighbor(nodeId(), targetConsumer);
        }

        /**
         * Calls the consumer once for each incoming neighbor of the given node.
         */
        default void forEachDistinctIncomingNeighbor(long nodeId, LongConsumer targetConsumer) {
            var prevTarget = new MutableLong(-1);
            graph().forEachInverseRelationship(nodeId, (ignored, targetNodeId) -> {
                if (prevTarget.longValue() != targetNodeId) {
                    targetConsumer.accept(targetNodeId);
                    prevTarget.setValue(targetNodeId);
                }
                return true;
            });
        }

    }
}


