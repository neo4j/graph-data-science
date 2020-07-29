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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

@GdlExtension
class UnionGraphTest {

    @GdlGraph(graphNamePrefix = "natural")
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String GDL = "()-->()-->()";

    @Inject
    TestGraph naturalGraph;

    @Inject
    TestGraph undirectedGraph;

    @Test
    void isUndirectedOnlyIfAllInnerGraphsAre() {
        Graph unionGraph1 = UnionGraph.of(List.of(naturalGraph, undirectedGraph));
        Graph unionGraph2 = UnionGraph.of(List.of(undirectedGraph, naturalGraph));

        assertFalse(unionGraph1.isUndirected());
        assertFalse(unionGraph2.isUndirected());
    }

}
