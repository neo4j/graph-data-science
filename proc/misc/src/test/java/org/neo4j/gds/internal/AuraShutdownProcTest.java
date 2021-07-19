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
package org.neo4j.gds.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.core.utils.io.file.csv.AutoloadFlagVisitor.AUTOLOAD_FILE_NAME;

public class AuraShutdownProcTest extends BaseProcTest {

    static String DB_CYPHER = "CREATE" +
                              "  (a:Label1 {prop1: 42})" +
                              ", (b:Label1)" +
                              ", (c:Label2 {prop2: 1337})" +
                              ", (d:Label2 {prop2: 10})" +
                              ", (e:Label2)" +
                              ", (a)-[:REL1]->(b)" +
                              ", (c)-[:REL2]->(d)";

    @TempDir
    Path tempDir;

    TestLog testLog;

    @BeforeEach
    void setup() throws Exception {
        GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class).register(new AuraShutdownProc());

        runQuery(DB_CYPHER);

        var graphStore1 = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("Label1", PropertyMappings.of(PropertyMapping.of("prop1"))))
            .addRelationshipProjection(RelationshipProjection.of("REL1", Orientation.NATURAL))
            .build()
            .graphStore();
        var createConfig1 = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph("")
            .graphName("first")
            .username("userA")
            .build();
        GraphStoreCatalog.set(createConfig1, graphStore1);

        var graphStore2 = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("Label2", PropertyMappings.of(PropertyMapping.of("prop2"))))
            .addRelationshipProjection(RelationshipProjection.of("REL2", Orientation.NATURAL))
            .build()
            .graphStore();
        var createConfig2 = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph("")
            .graphName("second")
            .username("userB")
            .build();
        GraphStoreCatalog.set(createConfig2, graphStore2);
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        testLog = new TestLog();
        builder.setUserLogProvider(new LogProvider() {
            @Override
            public Log getLog(Class<?> loggingClass) {
                return testLog;
            }

            @Override
            public Log getLog(String name) {
                return testLog;
            }
        });
        builder.setConfig(GraphStoreExportSettings.export_location_setting, tempDir);
    }

    @Test
    void shouldWriteAutoloadFlag() {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        assertCypherResult(shutdownQuery, List.of(Map.of("done", true)));

        assertThat(tempDir.resolve("first")).isDirectoryContaining("glob:**" + AUTOLOAD_FILE_NAME);
        assertThat(tempDir.resolve("second")).isDirectoryContaining("glob:**" + AUTOLOAD_FILE_NAME);
    }

    @Test
    void shouldPersistGraphStores() {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        assertCypherResult(shutdownQuery, List.of(Map.of("done", true)));

        assertThat(tempDir.resolve("first"))
            .isDirectoryContaining("glob:**/.userinfo")
            .isDirectoryContaining("glob:**/graph_info.csv")
            .isDirectoryContaining("glob:**/node-schema.csv")
            .isDirectoryContaining("glob:**/relationship-schema.csv")
            .isDirectoryContaining("glob:**/nodes_Label1_header.csv")
            .isDirectoryContaining("regex:.+/nodes_Label1_\\d+.csv")
            .isDirectoryContaining("glob:**/relationships_REL1_header.csv")
            .isDirectoryContaining("regex:.+/relationships_REL1_\\d+.csv");

        assertThat(tempDir.resolve("second"))
            .isDirectoryContaining("glob:**/.userinfo")
            .isDirectoryContaining("glob:**/graph_info.csv")
            .isDirectoryContaining("glob:**/node-schema.csv")
            .isDirectoryContaining("glob:**/relationship-schema.csv")
            .isDirectoryContaining("glob:**/nodes_Label2_header.csv")
            .isDirectoryContaining("regex:.+/nodes_Label2_\\d+.csv")
            .isDirectoryContaining("glob:**/relationships_REL2_header.csv")
            .isDirectoryContaining("regex:.+/relationships_REL2_\\d+.csv");

        assertThat(testLog.getMessages(TestLog.INFO))
            .anySatisfy(msg -> assertThat(msg)
                .matches(
                    "Shutdown happened within the given timeout, it took \\d+ seconds and the provided timeout as 42 seconds."
                ));
    }

    @Test
    void shouldCollectErrorsWhenPersistingGraphStores() throws IOException {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        var first = tempDir.resolve("first");
        Files.createDirectories(first);

        assertCypherResult(shutdownQuery, List.of(Map.of("done", false)));

        assertThat(testLog.getMessages(TestLog.WARN))
            .contains(
                "GraphStore persistence failed on graph first for user userA - The specified export directory already exists.");

        assertThat(first).isEmptyDirectory();

        assertThat(tempDir.resolve("second"))
            .isDirectoryContaining("glob:**/graph_info.csv")
            .isDirectoryContaining("glob:**/node-schema.csv")
            .isDirectoryContaining("glob:**/relationship-schema.csv")
            .isDirectoryContaining("glob:**/nodes_Label2_header.csv")
            .isDirectoryContaining("regex:.+/nodes_Label2_\\d+.csv")
            .isDirectoryContaining("glob:**/relationships_REL2_header.csv")
            .isDirectoryContaining("regex:.+/relationships_REL2_\\d+.csv");
    }

    @Test
    void shouldBringTheDatabaseIntoAStateThatIsSafeToRestart() throws Exception {
        GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class).register(new AuraMaintenanceFunction());
        var pollQuery = "RETURN gds.internal.safeToRestart() AS safeToRestart";
        var shutdownQuery = "CALL gds.internal.shutdown()";

        assertCypherResult(pollQuery, List.of(Map.of("safeToRestart", false)));

        runQuery(shutdownQuery);

        assertCypherResult(pollQuery, List.of(Map.of("safeToRestart", true)));
    }

    @Test
    void shouldLogAtStartAndForEachExportedGraph() {
        var numberOfGraphs = GraphStoreCatalog.graphStoresCount();
        var shutdownQuery = "CALL gds.internal.shutdown()";

        runQuery(shutdownQuery);

        var messages = testLog.getMessages(TestLog.INFO);

        assertThat(messages).contains("Preparing for shutdown");

        var exportCompletedMessages = messages.stream()
            .filter(it -> it.startsWith("Export completed"))
            .collect(toList());

        assertThat(exportCompletedMessages.size()).isEqualTo(numberOfGraphs);
    }
}
