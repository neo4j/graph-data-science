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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;

import java.util.function.LongToIntFunction;

public final class AdjacencyTestUtils {

    public static int degree(long nodeId, AdjacencyList adjacencyList) {
        return adjacencyList.degree(nodeId);
    }

    public static long[] targets(long nodeId, AdjacencyList adjacencyList) {
        var degree = adjacencyList.degree(nodeId);
        var targets = new long[degree];
        var cursor = adjacencyList.adjacencyCursor(nodeId);
        int i = 0;
        while (cursor.hasNextVLong()) {
            targets[i++] = cursor.nextVLong();
        }
        return targets;
    }

    public static double[] properties(
        long nodeId,
        AdjacencyProperties adjacencyProperties,
        LongToIntFunction degreeFn
    ) {
        var degree = degreeFn.applyAsInt(nodeId);
        var properties = new double[degree];
        var cursor = adjacencyProperties.propertyCursor(nodeId);
        int i = 0;
        while (cursor.hasNextLong()) {
            properties[i++] = Double.longBitsToDouble(cursor.nextLong());
        }
        return properties;
    }

    private AdjacencyTestUtils() {}
}
