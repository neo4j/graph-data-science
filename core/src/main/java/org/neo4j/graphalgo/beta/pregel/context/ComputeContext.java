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

/**
 * A context that is used during the computation. It allows an implementation
 * to send messages to other nodes and change the state of the currently
 * processed node.
 */
public final class ComputeContext<CONFIG extends PregelConfig> extends NodeCentricContext<CONFIG> {

    public ComputeContext(ComputeStep<CONFIG> computeStep, CONFIG config) {
        super(computeStep, config);
        this.sendMessagesFunction = config.relationshipWeightProperty() == null
            ? computeStep::sendToNeighbors
            : computeStep::sendToNeighborsWeighted;
    }

    private final SendMessagesFunction sendMessagesFunction;

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a double
     */
    public double doubleNodeValue(String key) {
        return computeStep.doubleNodeValue(key, nodeId);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a long
     */
    public long longNodeValue(String key) {
        return computeStep.longNodeValue(key, nodeId);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a long array
     */
    public long[] longArrayNodeValue(String key) {
        return computeStep.longArrayNodeValue(key, nodeId);
    }

    /**
     * Returns the node value for the given node-id and node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a long array
     */
    public long[] longArrayNodeValue(String key, long id) {
        return computeStep.longArrayNodeValue(key, id);
    }

    /**
     * Returns the node value for the given node schema key.
     *
     * @throws IllegalArgumentException if the key does not exist or the value is not a double array
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
