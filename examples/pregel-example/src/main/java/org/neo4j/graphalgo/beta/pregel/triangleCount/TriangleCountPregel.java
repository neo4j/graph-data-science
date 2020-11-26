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
package org.neo4j.graphalgo.beta.pregel.triangleCount;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;

/**
 * ! Assuming an unweighted graph
 */
@PregelProcedure(name = "example.pregel.triangleCount", modes = {GDSMode.STREAM})
public class TriangleCountPregel implements PregelComputation<TriangleCountPregelConfig> {

    public static final String TRIANGLE_COUNT = "TRIANGLES";
    private static final String NEIGHBOURS = "NEIGHBOURS";

    @Override
    public PregelSchema schema() {
        return new PregelSchema.Builder()
            .add(TRIANGLE_COUNT, ValueType.LONG)
            .add(NEIGHBOURS, ValueType.LONG_ARRAY, PregelSchema.Visibility.PRIVATE)
            .build();
    }

    @Override
    public void compute(PregelContext.ComputeContext<TriangleCountPregelConfig> context, Pregel.Messages messages) {
        if (context.isInitialSuperstep()) {
            context.setNodeValue(TRIANGLE_COUNT, 0);
            // assuming out-degree == in-degree
            context.setNodeValue(NEIGHBOURS, new long[context.degree()]);
            // send node id to each neighbour -> build neighbour set for each node
            context.sendToNeighbors(context.nodeId());
        } else if (context.superstep() == Phase.GATHER_NEIGHBORS.step) {
            long[] neighbours = context.longArrayNodeValue(NEIGHBOURS);
            int idx = 0;
            for (Double message : messages) {
                neighbours[idx] = message.longValue();
                idx++;
            }
        } else if (context.superstep() == Phase.MERGE_NEIGHBORS.step) {
            long[] neighbours = context.longArrayNodeValue(NEIGHBOURS);
            var isNeighbourFromA = new BitSet(context.nodeCount());
            for (long nodeB : neighbours) {
                isNeighbourFromA.set(nodeB);
            }

            long nodeA = context.nodeId();
            long trianglesFromNodeA = 0;
            long lastNodeB = -1;
            for (long nodeB : neighbours) {
                if (nodeB == lastNodeB) {
                    continue;
                }
                lastNodeB = nodeB;

                if (nodeB > nodeA) {
                    long[] followingNeighbours = context.longArrayNodeValue(NEIGHBOURS, nodeB);
                    long lastNodeC = -1;

                    // find common neighbors
                    // check indexed neighbours of A
                    for (long nodeC : followingNeighbours) {
                        if (lastNodeC != nodeC && nodeC > nodeB && isNeighbourFromA.get(nodeC)) {
                            trianglesFromNodeA++;
                            lastNodeC = nodeC;
                            context.sendTo(nodeB, 1);
                            context.sendTo(nodeC, 1);
                        }
                    }
                }
            }
            context.setNodeValue(TRIANGLE_COUNT, trianglesFromNodeA);
        } else if (context.superstep() == Phase.COUNT_TRIANGLES.step) {
            // free arrays again
            context.setNodeValue(NEIGHBOURS, (long[]) null);
            long triangles = context.longNodeValue(TRIANGLE_COUNT);
            for (Double ignored : messages) {
                triangles++;
            }

            context.setNodeValue(TRIANGLE_COUNT, triangles);
            context.voteToHalt();
        }
    }

    enum Phase {
        GATHER_NEIGHBORS(1),
        MERGE_NEIGHBORS(2),
        COUNT_TRIANGLES(3);

        final long step;

        Phase(int i) {
            step = i;
        }
    }
}
