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

package org.neo4j.graphalgo.triangle;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;

public abstract class TriangleBaseProc<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends AlgoBaseConfig>
    extends AlgoBaseProc<A, RESULT, CONFIG> {

    static final String DESCRIPTION =
        "Triangle counting is a community detection graph algorithm that is used to " +
        "determine the number of triangles passing through each node in the graph.";

    @Override
    protected void validateGraphCreateConfig(GraphCreateConfig graphCreateConfig) {
        graphCreateConfig.relationshipProjection().projections().entrySet().stream()
            .filter(entry -> entry.getValue().projection() != Projection.UNDIRECTED)
            .forEach(entry -> {
                throw new IllegalArgumentException(String.format(
                    "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses projection `%s`",
                    entry.getKey().name,
                    entry.getValue().projection()
                ));
            });
    }
}
