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
package org.neo4j.gds.triangle;

final class Constants {
    static final String LOCAL_CLUSTERING_COEFFICIENT_DESCRIPTION = "The local clustering coefficient is a metric quantifying how connected the neighborhood of a node is.";

    static final String TRIANGLE_COUNT_DESCRIPTION =
        "Triangle counting is a community detection graph algorithm that is used to " +
            "determine the number of triangles passing through each node in the graph.";

    static final String TRIANGLE_STREAM_DESCRIPTION = "Triangles streams the nodeIds of each triangle in the graph.";

    private Constants() {}
}
