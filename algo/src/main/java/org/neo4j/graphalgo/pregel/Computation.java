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
package org.neo4j.graphalgo.pregel;


import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.graphdb.Direction;

public abstract class Computation {

    private Pregel.ComputeStep computeStep;

    protected Direction getMessageDirection() {
        return Direction.OUTGOING;
    }

    protected double getDefaultNodeValue() {
        return -1.0;
    }

    void setComputeStep(final Pregel.ComputeStep computeStep) {
        this.computeStep = computeStep;
    }

    private static final double[] NO_MESSAGES = new double[0];

    protected void computeOnQueue(final long nodeId, MpscLinkedQueue<Double> messages) {
        if (messages == null) {
            compute(nodeId, NO_MESSAGES);
        } else {
            double[] messageArray = new double[messages.size()];
            for (int i = 0; i < messageArray.length; i++) {
                messageArray[i] = messages.poll();
            }
            compute(nodeId, messageArray);
        }
    }

    protected abstract void compute(final long nodeId, double[] messages);

    protected int getSuperstep() {
        return computeStep.getIteration();
    }

    protected void sendMessages(final long nodeId , final double message) {
        computeStep.sendMessages(nodeId, message, getMessageDirection());
    }

    protected double getValue(final long nodeId) {
        return computeStep.getNodeValue(nodeId);
    }

    protected void setValue(final long nodeId, final double value) {
        computeStep.setNodeValue(nodeId, value);
    }

    protected int getDegree(final long nodeId) {
        return computeStep.getDegree(nodeId, getMessageDirection());
    }
}
