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

package org.neo4j.graphalgo.beta.pregel.paths;

import org.neo4j.graphalgo.beta.pregel.Computation;

import java.util.Queue;

public class SSSPComputation extends Computation {

    private final long startNode;

    public SSSPComputation(long startNode) {
        this.startNode = startNode;
    }

    @Override
    protected boolean supportsAsynchronousParallel() {
        return true;
    }

    @Override
    protected void compute(long nodeId, Queue<Double> messages) {
        if (getSuperstep() == 0) {
            if (nodeId == startNode) {
                setNodeValue(nodeId, 0);
                sendMessages(nodeId, 1);
            } else {
                setNodeValue(nodeId, Long.MAX_VALUE);
            }
        } else {
            // This is basically the same message passing as WCC (except the new message)
            long newDistance = (long) getNodeValue(nodeId);
            boolean hasChanged = false;

            if (messages != null) {
                Double message;
                while ((message = messages.poll()) != null) {
                    if (message < newDistance) {
                        newDistance = message.longValue();
                        hasChanged = true;
                    }
                }
            }

            if (hasChanged) {
                setNodeValue(nodeId, newDistance);
                sendMessages(nodeId, newDistance + 1);
            }

            voteToHalt(nodeId);
        }

    }
}
