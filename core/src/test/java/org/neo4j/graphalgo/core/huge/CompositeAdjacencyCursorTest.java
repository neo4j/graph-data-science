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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.Graph;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CompositeAdjacencyCursorTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE " +
        "  (a)" +
        ", (b)" +
        ", (c)" +
        ", (a)-[:REL1]->(b)" +
        ", (a)-[:REL1]->(c)" +
        ", (a)-[:REL2]->(b)";

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldIterateInOrder() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addRelationshipType("REL1")
            .addRelationshipType("REL2")
            .build()
            .graph();

        assertTrue(graph instanceof UnionGraph);

        AdjacencyCursor adjacencyCursor = graph.adjacencyList().decompressingCursor(0);

        long lastNodeId = 0;
        while (adjacencyCursor.hasNextVLong()) {
            assertTrue(lastNodeId <= adjacencyCursor.nextVLong());
        }
    }
}