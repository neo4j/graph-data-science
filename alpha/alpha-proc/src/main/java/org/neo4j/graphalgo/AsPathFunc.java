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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;
import java.util.stream.IntStream;

public class AsPathFunc {

    @Context
    public GraphDatabaseAPI api;

    @UserFunction("gds.util.asPath")
    @Description("CALL gds.util.asPath - Return a path object for the provided node ids and weights.")
    public Path asPath(
        @Name(value = "nodeIds", defaultValue = "") List<Long> nodes,
        @Name(value = "weights", defaultValue = "") List<Double> weights,
        @Name(value = "cumulativeWeights", defaultValue = "true") boolean cumulativeWeights
    ) {
        if (nodes.size() <= 0) {
            return WalkPath.EMPTY;
        }

        long[] ids = nodes.stream().mapToLong(l -> l).toArray();

        if (weights.isEmpty()) {
            return WalkPath.toPath(api, ids);
        }

        if (cumulativeWeights && nodes.size() != weights.size()) {
            throw new IllegalArgumentException(message(weights.size(), nodes.size(), "size of 'nodeIds'"));
        }
        if (!cumulativeWeights && nodes.size() - 1 != weights.size()) {
            throw new IllegalArgumentException(message(weights.size(), nodes.size() - 1, "size of 'nodeIds' - 1"));
        }

        return cumulativeWeights
            ? WalkPath.toPath(api, ids, IntStream.range(0, weights.size() - 1).mapToDouble(i -> weights.get(i + 1) - weights.get(i)).toArray())
            : WalkPath.toPath(api, ids, weights.stream().mapToDouble(d -> d).toArray());
    }

    private String message(int actualSize, int expectedSize, String explanation) {
        return String.format(
            "'weights' contains %d values, but %d values were expected (%s)",
            actualSize,
            expectedSize,
            explanation
        );
    }
}
