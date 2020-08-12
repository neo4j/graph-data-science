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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.NodePropertyContainer;

import java.util.Set;

public final class PregelContext<CONFIG extends PregelConfig> {

    private final Pregel.ComputeStep<CONFIG> computeStep;
    private final CONFIG config;
    private final SendMessageFunction sendMessageFunction;
    private final NodePropertyContainer nodePropertyContainer;

    PregelContext(
        Pregel.ComputeStep<CONFIG> computeStep,
        CONFIG config,
        NodePropertyContainer nodePropertyContainer
    ) {
        this.computeStep = computeStep;
        this.config = config;
        this.nodePropertyContainer = nodePropertyContainer;
        this.sendMessageFunction = config.relationshipWeightProperty() == null
            ? computeStep::sendMessages
            : computeStep::sendWeightedMessages;
    }

    public Set<String> nodePropertyKeys() {
        return this.nodePropertyContainer.availableNodeProperties();
    }

    public NodeProperties nodeProperties(String key) {
        return this.nodePropertyContainer.nodeProperties(key);
    }

    public CONFIG getConfig() {
        return config;
    }

    public void voteToHalt(long nodeId) {
        computeStep.voteToHalt(nodeId);
    }

    public boolean isSeeded() {
        return config.isIncremental();
    }

    public boolean isInitialSuperstep() {
        return getSuperstep() == 0;
    }

    public int getSuperstep() {
        return computeStep.getIteration();
    }

    public double getNodeValue(long nodeId) {
        return computeStep.getNodeValue(nodeId);
    }

    public double doubleNodeValue(String key, long nodeId) {
        return computeStep.doubleNodeValue(key, nodeId);
    }

    public long longNodeValue(String key, long nodeId) {
        return computeStep.longNodeValue(key, nodeId);
    }

    public void setNodeValue(long nodeId, double value) {
        computeStep.setNodeValue(nodeId, value);
    }

    public void setNodeValue(String key, long nodeId, double value) {
        computeStep.setNodeValue(key, nodeId, value);
    }

    public void setNodeValue(String key, long nodeId, long value) {
        computeStep.setNodeValue(key, nodeId, value);
    }

    public void sendMessages(long nodeId, double message) {
        sendMessageFunction.sendMessage(nodeId, message);
    }

    public long getNodeCount() {
        return computeStep.getNodeCount();
    }

    public long getRelationshipCount() {
        return computeStep.getRelationshipCount();
    }

    public int getDegree(long nodeId) {
        return computeStep.getDegree(nodeId);
    }

    public double getInitialNodeValue() {
        return config.initialNodeValue();
    }

    @FunctionalInterface
    interface SendMessageFunction {

        void sendMessage(long nodeId, double message);
    }
}
