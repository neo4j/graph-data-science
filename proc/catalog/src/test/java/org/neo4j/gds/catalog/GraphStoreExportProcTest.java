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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.Config;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.compat.DatabaseMode;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.SettingProxy;
import org.neo4j.gds.core.GraphStoreExportSettings;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.core.utils.io.file.GraphStoreExporterUtil.EXPORT_DIR;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphStoreExportProcTest extends BaseProcTest {

    @TempDir
    Path tempDir;

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { prop1: 0, prop2: 42, prop3: 'knock', prop4: 'follow' })" +
        ", (b { prop1: 1, prop2: 43, prop3: 'knock', prop4: 'the'    })" +
        ", (c { prop1: 2, prop2: 44, prop3: 'neo',   prop4: 'white'  })" +
        ", (d { prop1: 3,                            prop4: 'rabbit' })" +
        ", (a)-[:REL1 { weight1: 42}]->(a)" +
        ", (a)-[:REL1 { weight1: 42}]->(b)" +
        ", (b)-[:REL2 { weight2: 42}]->(a)" +
        ", (b)-[:REL2 { weight2: 42}]->(c)" +
        ", (c)-[:REL3 { weight3: 42}]->(d)" +
        ", (d)-[:REL3 { weight3: 42}]->(a)";

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setFileSystem(new DefaultFileSystemAbstraction());
        builder.setConfig(GraphStoreExportSettings.export_location_setting, tempDir);
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, GraphStoreExportProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @Test
    void exportGraph() {
        projectGraph();

        var exportQuery = "CALL gds.graph.export('test-graph', {dbName: 'test-db'})";

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals("test-db", row.getString("dbName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(8, row.getNumber("nodePropertyCount").longValue());
            assertEquals(6, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThan(0L);
        });
    }

    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @Test
    void exportGraphWithAdditionalNodeProperties() {
        projectGraph();

        var exportQuery = "CALL gds.graph.export(" +
                          "  'test-graph', {" +
                          "    dbName: 'test-db'," +
                          "    additionalNodeProperties: [" +
                          "      {prop3: {defaultValue: 'foo'}}," +
                          "      {prop4: {defaultValue: 'bar'}}" +
                          "    ]" +
                          "  }" +
                          ")";

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals("test-db", row.getString("dbName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(16, row.getNumber("nodePropertyCount").longValue());
            assertEquals(6, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThan(0L);
        });
    }

    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_8_drop10)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_9_drop10)
    @Test
    void exportGraphWithAdditionalNodePropertiesShortHandSyntax() {
        projectGraph();

        var exportQuery = "CALL gds.graph.export(" +
                          "  'test-graph', {" +
                          "    dbName: 'test-db'," +
                          "    additionalNodeProperties: [" +
                          "      'prop3'," +
                          "      'prop4'" +
                          "    ]" +
                          "  }" +
                          ")";

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals("test-db", row.getString("dbName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(16, row.getNumber("nodePropertyCount").longValue());
            assertEquals(6, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThan(0L);
        });
    }

    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    @Test
    void exportGraphWithAdditionalNodePropertiesDuplicateProperties() {
        projectGraph();

        var exportQuery = "CALL gds.graph.export(" +
                          "  'test-graph', {" +
                          "    dbName: 'test-db'," +
                          "    additionalNodeProperties: [" +
                          "      'prop1'," +
                          "      'prop2'" +
                          "    ]" +
                          "  }" +
                          ")";

        assertThatCode(() -> runQuery(exportQuery))
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "The following provided additional node properties are already present in the in-memory graph: prop1 and prop2");
    }

    @Test
    void exportCsv() {
        projectGraph();

        var exportQuery = "CALL gds.beta.graph.export.csv('test-graph', {exportName: 'export'})";

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals("export", row.getString("exportName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(8, row.getNumber("nodePropertyCount").longValue());
            assertEquals(6, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThan(0L);
        });
    }

    @Test
    void exportCsvWithAdditionalNodeProperties() {
        projectGraph();

        var exportQuery = "CALL gds.beta.graph.export.csv(" +
                          "  'test-graph', {" +
                          "    exportName: 'export'," +
                          "    additionalNodeProperties: [" +
                          "      {prop3: {defaultValue: 'foo'}}," +
                          "      {prop4: {defaultValue: 'bar'}}" +
                          "    ]" +
                          "  }" +
                          ")";

        runQueryWithRowConsumer(exportQuery, row -> {
            assertEquals("export", row.getString("exportName"));
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(6, row.getNumber("relationshipCount").longValue());
            assertEquals(3, row.getNumber("relationshipTypeCount").longValue());
            assertEquals(16, row.getNumber("nodePropertyCount").longValue());
            assertEquals(6, row.getNumber("relationshipPropertyCount").longValue());
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThan(0L);
        });
    }

    @Test
    void failsWhenTheExportDirectoryAlreadyExists() throws IOException {
        var exportName = "export";
        Files.createDirectories(tempDir.resolve(EXPORT_DIR).resolve(exportName));

        projectGraph();

        var exportQuery = formatWithLocale(
            "CALL gds.beta.graph.export.csv('test-graph', {" +
            "  exportName: '%s'" +
            "})",
            exportName
        );

        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(exportQuery)
        );
        var pattern = formatWithLocale("The specified export directory '[^']+/%s' already exists\\.", exportName);
        assertThat(rootCause(exception)).hasMessageMatching(pattern);
    }

    @Test
    void failsWhenTryingToEscapeExportLocation() {
        var exportName = "../export";

        projectGraph();

        var exportQuery = formatWithLocale(
            "CALL gds.beta.graph.export.csv('test-graph', {" +
            "  exportName: '%s'" +
            "})",
            exportName
        );

        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(exportQuery)
        );

        assertThat(rootCause(exception)).hasMessage(
            "Illegal parameter value for parameter exportName '../export'. It attempts to write into a forbidden directory."
        );
    }

    @Test
    void failIfExportLocationIsNotSet() {
        projectGraph();

        GraphDatabaseApiProxy
            .resolveDependency(db, Config.class)
            .set(GraphStoreExportSettings.export_location_setting, null);

        var exportQuery =
            "CALL gds.beta.graph.export.csv('test-graph', {" +
            "  exportName: 'export'" +
            "})";

        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(exportQuery)
        );

        assertThat(rootCause(exception)).hasMessage(
            "The configuration option 'gds.export.location' must be set."
        );
    }


    @DisableForNeo4jVersion(Neo4jVersion.V_4_3)
    @Test
    void exportCsvWithAdditionalNodePropertiesDuplicateProperties() {
        projectGraph();

        var exportQuery = "CALL gds.beta.graph.export.csv(" +
                          "  'test-graph', {" +
                          "    exportName: 'export'," +
                          "    additionalNodeProperties: [" +
                          "      'prop1'," +
                          "      'prop2'" +
                          "    ]" +
                          "  }" +
                          ")";

        assertThatCode(() -> runQuery(exportQuery))
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "The following provided additional node properties are already present in the in-memory graph: prop1 and prop2");
    }

    @Test
    void failsCsvExportWhenRunningOnCluster() {
        var exportName = "export";
        var config = GraphDatabaseApiProxy.resolveDependency(db, Config.class);
        SettingProxy.setDatabaseMode(config, DatabaseMode.READ_REPLICA);

        projectGraph();

        var exportQuery = formatWithLocale(
            "CALL gds.beta.graph.export.csv('test-graph', {" +
            "  exportName: '%s'" +
            "})",
            exportName
        );

        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(exportQuery)
        );
        assertThat(rootCause(exception))
            .hasMessageContaining("The requested operation")
            .hasMessageContaining("(Export a graph to CSV)")
            .hasMessageContaining("is not available while running Neo4j Graph Data Science library on a Neo4j Causal Cluster."
        );
    }

    @Test
    void failsDatabaseExportWhenRunningOnCluster() {
        var exportName = "export";
        var config = GraphDatabaseApiProxy.resolveDependency(db, Config.class);
        SettingProxy.setDatabaseMode(config, DatabaseMode.READ_REPLICA);

        projectGraph();

        var exportQuery = formatWithLocale(
            "CALL gds.graph.export('test-graph', {" +
            "  dbName: '%s'" +
            "})",
            exportName
        );

        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(exportQuery)
        );
        assertThat(rootCause(exception))
            .hasMessageContaining("The requested operation")
            .hasMessageContaining("(Export a graph to Neo4j database)")
            .hasMessageContaining(
                "is not available while running Neo4j Graph Data Science library on a Neo4j Causal Cluster."
            );

    }

    @Test
    void csvEstimation() {
        projectGraph();

        var exportQuery =
            "CALL gds.beta.graph.export.csv.estimate('test-graph', {" +
            "  exportName: 'export'" +
            "})";

        runQueryWithRowConsumer(exportQuery, row -> {
            assertThat(row.getNumber("bytesMin").longValue()).isBetween(0L, 100L);
            assertThat(row.getNumber("bytesMax").longValue()).isBetween(0L, 100L);
        });
    }

    @Test
    void failCsvEstimationWhenRunningOnCluster() {
        var config = GraphDatabaseApiProxy.resolveDependency(db, Config.class);
        SettingProxy.setDatabaseMode(config, DatabaseMode.READ_REPLICA);

        projectGraph();

        var exportQuery =
            "CALL gds.beta.graph.export.csv.estimate('test-graph', {" +
            "  exportName: 'export'" +
            "})";

        var exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(exportQuery)
        );
        assertThat(rootCause(exception))
            .hasMessageContaining("The requested operation")
            .hasMessageContaining("(Estimation for exporting a graph to CSV)")
            .hasMessageContaining(
                "is not available while running Neo4j Graph Data Science library on a Neo4j Causal Cluster."
            );
    }

    private void projectGraph() {
        runQuery(GdsCypher.call("test-graph")
            .graphProject()
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
            .yields());
    }
}
