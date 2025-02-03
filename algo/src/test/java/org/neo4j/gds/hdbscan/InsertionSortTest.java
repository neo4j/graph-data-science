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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeObjectArray;

import static org.assertj.core.api.Assertions.assertThat;

class InsertionSortTest {

    @Test
    void shouldSort() {

        var edges = HugeObjectArray.newArray(Edge.class, 8);
        edges.set(0, new Edge(2, 4, 3.1622776601683795));
        edges.set(1, new Edge(6, 7, 1.0));
        edges.set(2, new Edge(9, 3, 18.601075237738275));
        edges.set(3, new Edge(7, 8, 2.23606797749979));
        edges.set(4, new Edge(2, 3, 1.1));
        edges.set(5, new Edge(5, 7, 2.0));
        edges.set(6, new Edge(1, 4, 1.4142135623730951));
        edges.set(7, new Edge(5, 4, 6.0));

        InsertionSort.sort(edges);

        assertThat(edges.toArray())
            .containsExactly(
                new Edge(6, 7, 1.0),
                new Edge(2, 3, 1.1),
                new Edge(1, 4, 1.4142135623730951),
                new Edge(5, 7, 2.0),
                new Edge(7, 8, 2.23606797749979),
                new Edge(2, 4, 3.1622776601683795),
                new Edge(5, 4, 6.0),
                new Edge(9, 3, 18.601075237738275)
            );


    }

}
