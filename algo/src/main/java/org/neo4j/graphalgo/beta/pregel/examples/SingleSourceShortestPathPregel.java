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
package org.neo4j.graphalgo.beta.pregel.examples;

import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelContext;

import java.util.Queue;

public class SingleSourceShortestPathPregel implements PregelComputation {

    private final long startNode;

    public SingleSourceShortestPathPregel(long startNode) {
        this.startNode = startNode;
    }

    @Override
    public void compute(PregelContext pregel, long nodeId, Queue<Double> messages) {
        if (pregel.isInitialSuperStep()) {
            if (nodeId == startNode) {
                pregel.setNodeValue(nodeId, 0);
                pregel.sendMessages(nodeId, 1);
            } else {
                pregel.setNodeValue(nodeId, Long.MAX_VALUE);
            }
        } else {
            // This is basically the same message passing as WCC (except the new message)
            long newDistance = (long) pregel.getNodeValue(nodeId);
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
                pregel.setNodeValue(nodeId, newDistance);
                pregel.sendMessages(nodeId, newDistance + 1);
            }

            pregel.voteToHalt(nodeId);
        }

    }
}
