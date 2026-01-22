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
package org.neo4j.gds.paths.prizesteiner;

class Constants {
    static final String PRIZE_STEINER_DESCRIPTION =
        "The prize collecting steiner tree algorithm accepts a weighted undirected graph where each node has a prize property. " +
            "It then attempts to find a spanning tree that maximizes the sum of prizes of the nodes in the tree"+
        "while minimizing the sum of weights for the tree's relationships as well as the sum of prizes for nodes outside the tree.";
}
