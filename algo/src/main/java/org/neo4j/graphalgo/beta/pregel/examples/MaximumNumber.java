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

import java.util.Arrays;
import java.util.Queue;

public class MaximumNumber implements PregelComputation {

    @Override
    public void compute(PregelContext pregel, long nodeId, Queue<Double> messages) {
        if (messages != null) {
            long oldValue = (long) pregel.getNodeValue(nodeId);
            long newValue = oldValue;

            Double nextMessage;
            while (!(nextMessage = messages.poll()).isNaN()) {
                if (nextMessage.longValue() > newValue) {
                    newValue = nextMessage.longValue();
                }
            }

            if (newValue != oldValue) {
                pregel.setNodeValue(nodeId, newValue);
                pregel.sendMessages(nodeId, newValue);
            }
        }
    }
}
