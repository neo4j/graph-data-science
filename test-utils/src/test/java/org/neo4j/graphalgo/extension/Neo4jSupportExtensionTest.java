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
package org.neo4j.graphalgo.extension;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.graphalgo.TestSupport.nodeIdByProperty;

class Neo4jSupportExtensionTest extends BaseTest {

    @org.neo4j.test.extension.Inject
    GraphDatabaseAPI neoInjectedDb;

    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE" +
                                            "  (a { id: 0 })" +
                                            ", (b { id: 1 })";

    @Test
    void shouldLoadGraphAndIdFunctionThroughExtension() {
        assertNotNull(db);
        assertEquals(db, neoInjectedDb);
        long idA = idFunction.of("a");
        long idB = idFunction.of("b");
        assertEquals(nodeIdByProperty(db, 0), idA);
        assertEquals(nodeIdByProperty(db, 1), idB);
    }

    @Test
    void shouldInjectNodeFunction() {
        var nodeA = nodeFunction.of("a");
        var nodeB = nodeFunction.of("b");

        QueryRunner.runQueryWithRowConsumer(
            db,
            "MATCH (n) WHERE n.id = 0 RETURN n",
            Map.of(),
            (tx, row) -> assertEquals(nodeA, row.getNode("n"))
        );
        QueryRunner.runQueryWithRowConsumer(
            db,
            "MATCH (n) WHERE n.id = 1 RETURN n",
            Map.of(),
            (tx, row) -> assertEquals(nodeB, row.getNode("n"))
        );
    }

}
