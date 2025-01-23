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
package org.neo4j.gds.hdbscan;

import java.util.Comparator;

final class UndirectedEdgeComparator implements Comparator<Edge> {

    UndirectedEdgeComparator() {}

    @Override
    public int compare(Edge o1, Edge o2) {
        var distanceComparison = Double.compare(o1.distance(), o2.distance());
        if (distanceComparison != 0) return distanceComparison;
        if ((o1.source() == o2.source()) && (o1.target() == o2.target())) return 0;
        if ((o1.source() == o2.target()) && (o1.target() == o2.source())) return 0;
        return -1;
    }
}
