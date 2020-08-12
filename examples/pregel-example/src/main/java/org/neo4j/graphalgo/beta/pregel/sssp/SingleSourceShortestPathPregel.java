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
package org.neo4j.graphalgo.beta.pregel.sssp;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;
import java.util.Queue;

import static org.neo4j.graphalgo.beta.pregel.sssp.SingleSourceShortestPathPregel.SingleSourceShortestPathPregelConfig;

@PregelProcedure(name = "example.pregel.sssp", modes = {GDSMode.STREAM})
public class SingleSourceShortestPathPregel implements PregelComputation<SingleSourceShortestPathPregelConfig> {

    @Override
    public void compute(PregelContext<SingleSourceShortestPathPregelConfig> pregel, long nodeId, Queue<Double> messages) {
        if (pregel.isInitialSuperstep()) {
            if (nodeId == pregel.getConfig().startNode()) {
                pregel.setNodeValue(nodeId, 0);
                pregel.sendMessages(nodeId, 1);
            } else {
                pregel.setNodeValue(nodeId, Double.MAX_VALUE);
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

    @ValueClass
    @Configuration("SingleSourceShortestPathPregelConfigImpl")
    @SuppressWarnings("immutables:subtype")
    interface SingleSourceShortestPathPregelConfig extends PregelConfig {
        @Value
        long startNode();

        static SingleSourceShortestPathPregelConfig of(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper userInput
        ) {
            return new SingleSourceShortestPathPregelConfigImpl(graphName, maybeImplicitCreate, username, userInput);
        }
    }
}
