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
package org.neo4j.graphalgo.beta.pregel.components;

import org.neo4j.graphalgo.beta.pregel.Computation;
import org.neo4j.graphdb.Direction;

import java.util.Queue;

// TODO: Inheritance would be an anti pattern for other languages (e.g. use compute closure)
// TODO: byte code javap (explore inlining)
public class WCCComputation extends Computation {

    @Override
    protected Direction getMessageDirection() {
        return Direction.BOTH;
    }

    @Override
    protected boolean supportsAsynchronousParallel() {
        return true;
    }

    @Override
    protected void compute(final long nodeId, Queue<Double> messages) {
        if (getSuperstep() == 0) {
            double currentValue = getNodeValue(nodeId);
            if (currentValue == getDefaultNodeValue()) {
                sendMessages(nodeId, nodeId);
                setNodeValue(nodeId, nodeId);
            } else {
                sendMessages(nodeId, currentValue);
            }
        } else {
            long newComponentId = (long) getNodeValue(nodeId);
            boolean hasChanged = false;

            // TODO: foreach consumer?
            if (messages != null) {
                Double message;
                while ((message = messages.poll()) != null) {
                    if (message < newComponentId) {
                        newComponentId = message.longValue();
                        hasChanged = true;
                    }
                }
            }

            if (hasChanged) {
                setNodeValue(nodeId, newComponentId);
                sendMessages(nodeId, newComponentId);
            }

            voteToHalt(nodeId);
        }
    }
}
