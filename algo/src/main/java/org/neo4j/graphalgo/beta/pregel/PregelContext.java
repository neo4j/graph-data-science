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

import org.neo4j.graphdb.Direction;

public final class PregelContext {

    private final Pregel.ComputeStep computeStep;
    private final PregelConfig config;

    PregelContext(Pregel.ComputeStep computeStep, PregelConfig config) {
        this.computeStep = computeStep;
        this.config = config;
    }

    public void voteToHalt(long nodeId) {
        computeStep.voteToHalt(nodeId);
    }

    public boolean isInitialSuperStep() {
        return getSuperstep() == 0;
    }

    public int getSuperstep() {
        return computeStep.getIteration();
    }

    public double getNodeValue(long nodeId) {
        return computeStep.getNodeValue(nodeId);
    }

    public void setNodeValue(long nodeId, double value) {
        computeStep.setNodeValue(nodeId, value);
    }

    public void sendMessages(long nodeId, double message) {
        sendMessages(nodeId, message, config.getMessageDirection());
    }

    public void sendMessages(long nodeId, double message, Direction direction) {
        computeStep.sendMessages(nodeId, message, direction);
    }

    public int getDegree(long nodeId) {
        return getDegree(nodeId, config.getMessageDirection());
    }

    public int getDegree(long nodeId, Direction direction) {
        return computeStep.getDegree(nodeId, direction);
    }

    public double getInitialNodeValue() {
        return config.getInitialNodeValue();
    }
}
