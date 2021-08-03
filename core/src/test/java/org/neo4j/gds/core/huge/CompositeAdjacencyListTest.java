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
package org.neo4j.gds.core.huge;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.api.Graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class CompositeAdjacencyListTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a)" +
        ", (b)" +
        ", (a)-[:T1]->(b)" +
        ", (a)-[:T2]->(c)";

    @Inject
    Graph graph;

    @Test
    void shouldIgnoreInputDegreeForCursor() {
        var adjacencyList = ((UnionGraph) graph).relationshipTopology();
        var cursor = adjacencyList.adjacencyCursor(0);
        assertEquals(2, cursor.remaining());
    }
}
