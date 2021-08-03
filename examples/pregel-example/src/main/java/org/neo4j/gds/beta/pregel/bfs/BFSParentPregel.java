/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.beta.pregel.bfs;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.Reducer;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;
import org.neo4j.gds.beta.pregel.context.ComputeContext;

import java.util.Optional;

/**
 * Setting the value for each node to the node-id of its parent.
 * If there are multiple parents at the discovery level/iteration, the parent with the minimum id is chosen.
 */
@PregelProcedure(name = "example.pregel.bfs.parent", modes = {GDSMode.STREAM})
public class BFSParentPregel implements PregelComputation<BFSPregelConfig> {

    public static final long NOT_FOUND = Long.MAX_VALUE;
    public static final String PARENT = "parent";

    @Override
    public PregelSchema schema(BFSPregelConfig config) {
        return new PregelSchema.Builder().add(PARENT, ValueType.LONG).build();
    }

    @Override
    public void compute(ComputeContext<BFSPregelConfig> context, Messages messages) {
        long nodeId = context.nodeId();

        if (context.isInitialSuperstep()) {
            if (nodeId == context.config().startNode()) {
                context.setNodeValue(PARENT, nodeId);
                context.sendToNeighbors(nodeId);
                context.voteToHalt();
            } else {
                context.setNodeValue(PARENT, NOT_FOUND);
            }
        } else {
            long currentParent = context.longNodeValue(PARENT);

            if (currentParent == NOT_FOUND) {
                for (var msg : messages) {
                    currentParent = Long.min(currentParent, msg.longValue());
                }

                // only send message if a parent existed
                if (currentParent != NOT_FOUND) {
                    context.setNodeValue(PARENT, currentParent);
                    context.sendToNeighbors(nodeId);
                }
            }
            context.voteToHalt();
        }
    }

    @Override
    public Optional<Reducer> reducer() {
        return Optional.of(new Reducer.Min());
    }
}

