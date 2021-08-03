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
 * Setting the value for each node to the level/iteration the node is discovered via BFS.
 */
@PregelProcedure(name = "example.pregel.bfs.level", modes = {GDSMode.STREAM})
public class BFSLevelPregel implements PregelComputation<BFSPregelConfig> {

    private static final long NOT_FOUND = -1;
    public static final String LEVEL = "LEVEL";

    @Override
    public PregelSchema schema(BFSPregelConfig config) {
        return new PregelSchema.Builder().add(LEVEL, ValueType.LONG).build();
    }

    @Override
    public void compute(ComputeContext<BFSPregelConfig> context, Messages messages) {
        if (context.isInitialSuperstep()) {
            if (context.nodeId() == context.config().startNode()) {
                context.setNodeValue(LEVEL, 0);
                context.sendToNeighbors(1);
                context.voteToHalt();
            } else {
                context.setNodeValue(LEVEL, NOT_FOUND);
            }
        } else if (messages.iterator().hasNext()) {
            long level = context.longNodeValue(LEVEL);
            if (level == NOT_FOUND) {
                level = messages.iterator().next().longValue();

                context.setNodeValue(LEVEL, level);
                context.sendToNeighbors(level + 1);
            }
        }
        context.voteToHalt();
    }

    @Override
    public Optional<Reducer> reducer() {
        return Optional.of(new Reducer() {
            @Override
            public double identity() {
                return -1;
            }

            @Override
            public double reduce(double current, double message) {
                return message;
            }
        });
    }
}
