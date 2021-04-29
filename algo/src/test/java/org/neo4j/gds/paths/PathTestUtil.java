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
package org.neo4j.gds.paths;

import org.neo4j.graphalgo.extension.IdFunction;

public final class PathTestUtil {

    public static PathResult expected(
        IdFunction idFunction,
        long index,
        double[] costs,
        String... nodes
    ) {
        return expected(idFunction, index, new long[0], costs, nodes);
    }

    public static PathResult expected(
        IdFunction idFunction,
        long index,
        long[] relationshipIds,
        double[] costs,
        String... nodes
    ) {
        var builder = ImmutablePathResult.builder()
            .index(index)
            .sourceNode(idFunction.of(nodes[0]))
            .targetNode(idFunction.of(nodes[nodes.length - 1]));

        var nodeIds = new long[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            nodeIds[i] = idFunction.of(nodes[i]);
        }

        return builder
            .costs(costs)
            .nodeIds(nodeIds)
            .relationshipIds(relationshipIds)
            .build();
    }

    private PathTestUtil() {}
}
