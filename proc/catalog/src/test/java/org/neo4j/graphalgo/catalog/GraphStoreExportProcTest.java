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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class GraphStoreExportProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { prop1: 0, prop2: 42 })" +
        ", (b { prop1: 1, prop2: 43 })" +
        ", (c { prop1: 2, prop2: 44 })" +
        ", (d { prop1: 3 })" +
        ", (a)-[:REL1 { weight1: 42}]->(a)" +
        ", (a)-[:REL1 { weight1: 42}]->(b)" +
        ", (b)-[:REL2 { weight2: 42}]->(a)" +
        ", (b)-[:REL2 { weight2: 42}]->(c)" +
        ", (c)-[:REL3 { weight3: 42}]->(d)" +
        ", (d)-[:REL3 { weight3: 42}]->(a)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, GraphStoreExportProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
   void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void exportGraph() {
        createGraph();

        var exportQuery = formatWithLocale(
            "CALL gds.graph.export('test-graph', {" +
            "  dbName: 'test-db'" +
            "})"
        );

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals("test-db", row.getString("dbName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(8, row.getNumber("nodePropertyCount").longValue());
            assertEquals(18, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue(), greaterThan(0L));
        });
    }

    @Test
    void exportCsv(@TempDir Path tempDir) {
        createGraph();

        var exportQuery = formatWithLocale(
            "CALL gds.graph.export.csv('test-graph', {" +
            "  exportLocation: '%s'" +
            "})"
        , tempDir);

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals(tempDir.toString(), row.getString("exportLocation"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(8, row.getNumber("nodePropertyCount").longValue());
            assertEquals(18, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue(), greaterThan(0L));
        });
    }

    private void createGraph() {
        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withNodeProperty("prop1")
            .withNodeProperty("prop2")
            .withRelationshipType("REL1", RelationshipProjection
                .of("REL1", Orientation.NATURAL)
                .withProperties(PropertyMappings.of(PropertyMapping.of("weight1")))
            )
            .withRelationshipType("REL2", RelationshipProjection
                .of("REL2", Orientation.NATURAL)
                .withProperties(PropertyMappings.of(PropertyMapping.of("weight2")))
            )
            .withRelationshipType("REL3", RelationshipProjection
                .of("REL3", Orientation.NATURAL)
                .withProperties(PropertyMappings.of(PropertyMapping.of("weight3")))
            )
            .graphCreate("test-graph")
            .yields());
    }
}
