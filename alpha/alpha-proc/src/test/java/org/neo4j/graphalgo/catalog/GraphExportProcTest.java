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
package org.neo4j.graphalgo.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.io.File;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphExportProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { prop1: 0, prop2: 42 })" +
        ", (b { prop1: 1, prop2: 43 })" +
        ", (c { prop1: 2, prop2: 44 })" +
        ", (d { prop1: 3 })" +
        ", (a)-[:REL]->(a)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(a)";

    @TempDir
    File tempDir;

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class, GraphExportProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void exportGraph() {
        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withNodeProperty("prop1")
            .withNodeProperty("prop2")
            .withAnyRelationshipType()
            .graphCreate("test-graph")
            .yields());

        String exportQuery = String.format(
            "CALL gds.alpha.graph.export('test-graph', {" +
            "  storeDir: '%s'," +
            "  dbName: 'test-db'" +
            "}) YIELD graphName, storeDir, dbName, nodeCount, relationshipCount, nodePropertyCount, writeMillis",
            tempDir.getAbsolutePath()
        );

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals(tempDir.getAbsolutePath(), row.getString("storeDir"));
            assertEquals("test-db", row.getString("dbName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(8, row.getNumber("nodePropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue(), greaterThan(0L));
        });
    }

}
