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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.NodePropertyContainer;

import java.util.Set;

public abstract class PregelContext<CONFIG extends PregelConfig> {

    private final CONFIG config;

    final Pregel.ComputeStep<CONFIG> computeStep;

    long nodeId;

    static <CONFIG extends PregelConfig> InitContext<CONFIG> initContext(
        Pregel.ComputeStep<CONFIG> computeStep,
        CONFIG config,
        NodePropertyContainer nodePropertyContainer
    ) {
        return new InitContext<>(computeStep, config, nodePropertyContainer);
    }

    static <CONFIG extends PregelConfig> ComputeContext<CONFIG> computeContext(
        Pregel.ComputeStep<CONFIG> computeStep,
        CONFIG config
    ) {
        return new ComputeContext<>(computeStep, config);
    }

    PregelContext(Pregel.ComputeStep<CONFIG> computeStep, CONFIG config) {
        this.computeStep = computeStep;
        this.config = config;
    }

    /**
     * Used internally by the framework to set the currently processed node.
     */
    void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * The identifier of the node that is currently processed.
     */
    public long nodeId() {
        return nodeId;
    }

    /**
     * Allows access to the user-defined Pregel configuration.
     */
    public CONFIG config() {
        return config;
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
     * Number of nodes in the input graph.
     */
    public long nodeCount() {
        return computeStep.nodeCount();
    }

    /**
     * Number of relationships in the input graph.
     */
    public long relationshipCount() {
        return computeStep.relationshipCount();
    }

    /**
     * Returns the degree (number of relationships) of the currently processed node.
     */
    public int degree() {
        return computeStep.degree(nodeId);
    }

    /**
     * A context that is used during the initialization phase, which is before the
     * first superstep is being executed. The init context allows accessing node
     * properties from the input graph which can be used to set initial node values
     * for the Pregel computation.
     */
    public static final class InitContext<CONFIG extends PregelConfig> extends PregelContext<CONFIG> {
        private final NodePropertyContainer nodePropertyContainer;

        InitContext(
            Pregel.ComputeStep<CONFIG> computeStep,
            CONFIG config,
            NodePropertyContainer nodePropertyContainer
        ) {
            super(computeStep, config);
            this.nodePropertyContainer = nodePropertyContainer;
        }

        /**
         * Returns the node property keys stored in the input graph.
         * These properties can be the result of previous computations
         * or part of node projections when creating the graph.
         */
        public Set<String> nodePropertyKeys() {
            return this.nodePropertyContainer.availableNodeProperties();
        }

        /**
         * Returns the property values for the given property key.
         * Property values can be used to access individual node
         * property values by using their node identifier.
         */
        public NodeProperties nodeProperties(String key) {
            return this.nodePropertyContainer.nodeProperties(key);
        }
    }

    /**
     * A context that is used during the computation. It allows an implementation
     * to send messages to other nodes and change the state of the currently
     * processed node.
     */
    public static final class ComputeContext<CONFIG extends PregelConfig> extends PregelContext<CONFIG> {

        ComputeContext(Pregel.ComputeStep<CONFIG> computeStep, CONFIG config) {
            super(computeStep, config);
            this.sendMessagesFunction = config.relationshipWeightProperty() == null
                ? computeStep::sendToNeighbors
                : computeStep::sendToNeighborsWeighted;
        }

        private final SendMessagesFunction sendMessagesFunction;

        /**
         * Returns the node value for the given node schema key.
         *
         * @throws java.lang.IllegalArgumentException if the key does not exist or the value is not a double
         */
        public double doubleNodeValue(String key) {
            return computeStep.doubleNodeValue(key, nodeId);
        }

        /**
         * Returns the node value for the given node schema key.
         *
         * @throws java.lang.IllegalArgumentException if the key does not exist or the value is not a long
         */
        public long longNodeValue(String key) {
            return computeStep.longNodeValue(key, nodeId);
        }

        /**
         * Returns the node value for the given node schema key.
         *
         * @throws java.lang.IllegalArgumentException if the key does not exist or the value is not a long array
         */
        public long[] longArrayNodeValue(String key) {
            return computeStep.longArrayNodeValue(key, nodeId);
        }

        /**
         * Returns the node value for the given node schema key.
         *
         * @throws java.lang.IllegalArgumentException if the key does not exist or the value is not a double array
         */
        public double[] doubleArrayNodeValue(String key) {
            return computeStep.doubleArrayNodeValue(key, nodeId);
        }

        /**
         * Notify the execution framework that this node intends
         * to stop the computation. If the node voted to halt
         * and has not received new messages in the next superstep,
         * the compute method will not be called for that node.
         * If a node receives messages, the vote to halt flag will
         * be ignored.
         */
        public void voteToHalt() {
            computeStep.voteToHalt(nodeId);
        }

        /**
         * Indicates if the current superstep is the first superstep.
         */
        public boolean isInitialSuperstep() {
            return superstep() == 0;
        }

        /**
         * Returns the current superstep (0-based).
         */
        public int superstep() {
            return computeStep.iteration();
        }

        /**
         * Sends the given message to all neighbors of the node.
         */
        public void sendToNeighbors(double message) {
            sendMessagesFunction.sendToNeighbors(nodeId, message);
        }

        /**
         * Sends the given message to the target node. The target
         * node can be any existing node id in the graph.
         *
         * @throws ArrayIndexOutOfBoundsException if the node is in the not in id space
         */
        public void sendTo(long targetNodeId, double message) {
            computeStep.sendTo(targetNodeId, message);
        }

        @FunctionalInterface
        interface SendMessagesFunction {
            void sendToNeighbors(long sourceNodeId, double message);
        }
    }
}
