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
package org.neo4j.graphalgo.beta.paths;

import org.neo4j.graphalgo.api.RelationshipProperties;
import org.neo4j.graphalgo.extension.IdFunction;

public final class PathTestUtil {

    public static PathResult expected(
        RelationshipProperties graph,
        IdFunction idFunction,
        long index,
        String... nodes
    ) {
        var builder = ImmutablePathResult.builder()
            .index(index)
            .sourceNode(idFunction.of(nodes[0]))
            .targetNode(idFunction.of(nodes[nodes.length - 1]));

        var nodeIds = new long[nodes.length];
        var costs = new double[nodes.length];

        var cost = 0.0;
        var prevNode = -1L;

        for (int i = 0; i < nodes.length; i++) {
            var currentNode = idFunction.of(nodes[i]);
            if (i > 0) {
                cost += graph.relationshipProperty(prevNode, currentNode);
            }
            prevNode = currentNode;
            nodeIds[i] = currentNode;
            costs[i] = cost;
        }

        return builder
            .totalCost(costs[costs.length - 1])
            .costs(costs)
            .nodeIds(nodeIds)
            .build();
    }

    private PathTestUtil() {}
}
