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
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.NodeSchemaBuilder;
import org.neo4j.graphalgo.beta.pregel.Pregel;
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

    static final String DISTANCE = "DISTANCE";

    @Override
    public Pregel.NodeSchema nodeSchema() {
        return new NodeSchemaBuilder().putElement(DISTANCE, ValueType.LONG).build();
    }

    @Override
    public void init(PregelContext.InitContext<SingleSourceShortestPathPregelConfig> context, long nodeId) {
        if (nodeId == context.getConfig().startNode()) {
            context.setNodeValue(DISTANCE, nodeId, 0);
        } else {
            context.setNodeValue(DISTANCE, nodeId, Long.MAX_VALUE);
        }
    }

    @Override
    public void compute(PregelContext.ComputeContext<SingleSourceShortestPathPregelConfig> context, long nodeId, Queue<Double> messages) {
        if (context.isInitialSuperstep()) {
            if (nodeId == context.getConfig().startNode()) {
                context.sendMessages(nodeId, 1);
            }
        } else {
            // This is basically the same message passing as WCC (except the new message)
            long newDistance = context.longNodeValue(DISTANCE, nodeId);
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
                context.setNodeValue(DISTANCE, nodeId, newDistance);
                context.sendMessages(nodeId, newDistance + 1);
            }

            context.voteToHalt(nodeId);
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
