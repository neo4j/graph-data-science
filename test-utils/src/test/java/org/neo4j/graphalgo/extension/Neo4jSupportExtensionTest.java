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
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Neo4jGraphExtension
class Neo4jSupportExtensionTest {

    @Inject
    GraphDatabaseAPI db;

    @Neo4jGraph
    private static final String DB_CYPHER = "" +
                                            "CREATE" +
                                            "  (a { id: 0 })" +
                                            ", (b { id: 1 })";

    @Inject
    IdFunction idFunction;

    @Test
    void shouldLoadGraph() {
        assertNotNull(db);
        long idA = idFunction.of("a");
        long idB = idFunction.of("b");
        QueryRunner.runQueryWithRowConsumer(db, "MATCH (n) WHERE n.id = 0 RETURN id(n) AS id", Map.of(), (tx, row) -> assertEqualIds(idA, row));
        QueryRunner.runQueryWithRowConsumer(db, "MATCH (n) WHERE n.id = 1 RETURN id(n) AS id", Map.of(), (tx, row) -> assertEqualIds(idB, row));
    }

    private void assertEqualIds(long expected, Result.ResultRow row) {
        assertEquals(expected, row.getNumber("id"));
    }

}