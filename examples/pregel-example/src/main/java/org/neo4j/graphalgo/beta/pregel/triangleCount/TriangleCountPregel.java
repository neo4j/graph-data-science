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

import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.NodeSchemaBuilder;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.annotation.GDSMode;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * ! Assuming an unweighted graph
 */
@PregelProcedure(name = "example.pregel.triangleCount", modes = {GDSMode.STREAM})
public class TriangleCountPregel implements PregelComputation<TriangleCountPregelConfig> {

    public static final String TRIANGLE_COUNT = "TRIANGLES";
    private static final String NEIGHBOURS = "NEIGHBOURS";

    @Override
    public Pregel.NodeSchema nodeSchema() {
        return new NodeSchemaBuilder()
            .putElement(TRIANGLE_COUNT, ValueType.LONG)
            .putElement(NEIGHBOURS, ValueType.LONG_ARRAY)
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
            // to later allow sorted merge
            Arrays.sort(neighbours);
            // dummy message to get next iteration
            context.sendTo(context.nodeId(), 0);
        } else if (context.superstep() == Phase.MERGE_NEIGHBORS.step) {
            long[] neighbours = context.longArrayNodeValue(NEIGHBOURS);
            long nodeA = context.nodeId();
            long trianglesFromNodeA = 0;

            // Consume dummy message
            Iterator<Double> iterator = messages.iterator();
            while(iterator.hasNext()) {
                iterator.next();
            }

            long lastNodeB = -1;
            for (long nodeB : neighbours) {
                if (nodeB == lastNodeB) {
                    continue;
                }

                lastNodeB = nodeB;

                if (nodeB > nodeA) {
                    long[] followingNeighbours = context.longArrayNodeValue(NEIGHBOURS, nodeB);
                    int i = 0, j = 0;

                    long lastNodeC = -1;

                    // find common neighbors
                    while (i < followingNeighbours.length && j < neighbours.length) {
                        long nodeCfromB = followingNeighbours[i];
                        long nodeCfromA = neighbours[j];
                        if (nodeCfromB < nodeCfromA)
                            i++;
                        else if (nodeCfromA < nodeCfromB)
                            j++;
                        else {
                            // only count each triangle once and avoid parallel rels
                            if (nodeCfromA > nodeB && lastNodeC != nodeCfromA) {
                                trianglesFromNodeA++;
                                lastNodeC = nodeCfromA;
                                context.sendTo(nodeB, 1);
                                context.sendTo(nodeCfromA, 1);
                            }

                            i++;
                            j++;
                        }
                    }
                }
            }
            context.setNodeValue(TRIANGLE_COUNT, trianglesFromNodeA);
        } else if (context.superstep() == Phase.COUNT_TRIANGLES.step) {
            // free arrays again
            context.setNodeValue(NEIGHBOURS, (long[]) null);
            long triangles = context.longNodeValue(TRIANGLE_COUNT);
            for (Double message : messages) {
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
