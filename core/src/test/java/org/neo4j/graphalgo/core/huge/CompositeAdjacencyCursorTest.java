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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class CompositeAdjacencyCursorTest extends BaseTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a)" +
        ", (b)" +
        ", (c)" +
        ", (a)-[:REL1]->(b)" +
        ", (a)-[:REL1]->(c)" +
        ", (a)-[:REL2]->(b)" +
        ", (a)-[:REL2]->(a)";

    private CompositeAdjacencyCursor adjacencyCursor;

    @Inject
    Graph graph;

    @BeforeEach
    void setup() {
        adjacencyCursor = (CompositeAdjacencyCursor) ((UnionGraph)graph).relationshipTopology().list().decompressingCursor(0);
    }

    @Test
    void shouldIterateInOrder() {
        long lastNodeId = 0;
        while (adjacencyCursor.hasNextVLong()) {
            assertTrue(lastNodeId <= adjacencyCursor.nextVLong());
        }
    }

    @Test
    void shouldSkipUntilLargerValue() {
        assertEquals(2, adjacencyCursor.skipUntil(1));
        assertFalse(adjacencyCursor.hasNextVLong());
    }

    @Test
    void shouldAdvanceUntilEqualValue() {
        assertEquals(1, adjacencyCursor.advance(1));
        assertEquals(1, adjacencyCursor.nextVLong());
        assertEquals(2, adjacencyCursor.nextVLong());
        assertFalse(adjacencyCursor.hasNextVLong());
    }

    @Test
    void shouldNotReturnLastValueWhenAdvanceExhaustsCursor() {
        assertEquals(2, adjacencyCursor.advance(2));
    }
}
