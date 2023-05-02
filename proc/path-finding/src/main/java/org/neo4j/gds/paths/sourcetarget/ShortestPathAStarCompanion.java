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
package org.neo4j.gds.paths.sourcetarget;

final class ShortestPathAStarCompanion {
    static final String ASTAR_DESCRIPTION =
        "The A* shortest path algorithm computes the shortest path between a pair of nodes. " +
            "It uses the relationship weight property to compare path lengths. " +
            "In addition, this implementation uses the haversine distance as a heuristic to converge faster.";

    private ShortestPathAStarCompanion() {}
}
