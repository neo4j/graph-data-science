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

public interface Aggregator {

    Aggregator NO_AGGREGATION = (s, t, w) -> .0;

    /**
     * aggregate weight between source and current node
     *
     * @param sourceNode     source node
     * @param currentNode    the current node
     * @param weightAtSource the weight that has been aggregated for the currentNode so far
     * @return new weight (e.g. weightAtSource + 1.)
     */
    double apply(long sourceNode, long currentNode, double weightAtSource);
}
