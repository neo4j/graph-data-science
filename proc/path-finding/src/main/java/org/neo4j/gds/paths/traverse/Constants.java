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
package org.neo4j.gds.paths.traverse;

final class Constants {
    static final String BFS_DESCRIPTION =
        "BFS is a traversal algorithm, which explores all of the neighbor nodes at " +
            "the present depth prior to moving on to the nodes at the next depth level.";
    static final String DFS_DESCRIPTION =
        "Depth-first search (DFS) is an algorithm for traversing or searching tree or graph data structures. " +
        "The algorithm starts at the root node (selecting some arbitrary node as the root node in the case of a graph) " +
        "and explores as far as possible along each branch before backtracking.";

    private Constants() {}
}
