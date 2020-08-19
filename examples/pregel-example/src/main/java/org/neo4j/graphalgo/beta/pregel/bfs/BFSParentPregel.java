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
package org.neo4j.graphalgo.beta.pregel.bfs;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.NodeSchemaBuilder;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;

/**
 * Setting the value for each node to the node-id of its parent.
 * If there are multiple parents at the discovery level/iteration, the parent with the minimum id is chosen.
 */
@PregelProcedure(name = "example.pregel.bfs", modes = {GDSMode.STREAM})
public class BFSParentPregel implements PregelComputation<BFSPregelConfig> {

    private static final long NOT_FOUND = Long.MAX_VALUE;
    public static final String PARENT = "parent";

    @Override
    public Pregel.NodeSchema nodeSchema() {
        return new NodeSchemaBuilder()
            .putElement(PARENT, ValueType.LONG)
            .build();
    }

    @Override
    public void compute(PregelContext.ComputeContext<BFSPregelConfig> context, long nodeId, Pregel.Messages messages) {
        if (context.isInitialSuperstep()) {
            if (nodeId == context.getConfig().startNode()) {
                context.setNodeValue(PARENT, nodeId, nodeId);
                context.sendMessages(nodeId, nodeId);
                context.voteToHalt(nodeId);
            } else {
                context.setNodeValue(PARENT, nodeId, NOT_FOUND);
            }
        } else {
            long currentParent = context.longNodeValue(PARENT, nodeId);

            if (currentParent == NOT_FOUND) {
                for (var msg : messages) {
                    currentParent = Long.min(currentParent, msg.longValue());
                }

                context.setNodeValue(PARENT, nodeId, currentParent);
                context.sendMessages(nodeId, nodeId);
            }
            context.voteToHalt(nodeId);
        }
    }
}

