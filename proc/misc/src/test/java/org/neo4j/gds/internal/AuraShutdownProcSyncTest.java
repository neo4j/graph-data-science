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
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.internal.AuraTestSupport.assertGraph;
import static org.neo4j.gds.internal.AuraTestSupport.assertGraphs;
import static org.neo4j.gds.internal.AuraTestSupport.assertModels;
import static org.neo4j.graphalgo.core.utils.io.file.csv.AutoloadFlagVisitor.AUTOLOAD_FILE_NAME;

class AuraShutdownProcSyncTest extends AuraShutdownBaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        super.setup();
        GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class).register(new AuraShutdownProc(true));
    }

    @Test
    void shouldWriteAutoloadFlag() {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        assertCypherResult(shutdownQuery, List.of(Map.of("submitted", true)));

        assertThat(tempDir.resolve("graphs/first")).isDirectoryContaining("glob:**" + AUTOLOAD_FILE_NAME);
        assertThat(tempDir.resolve("graphs/second")).isDirectoryContaining("glob:**" + AUTOLOAD_FILE_NAME);
    }

    @Test
    void shouldPersistGraphStores() throws IOException {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        assertCypherResult(shutdownQuery, List.of(Map.of("submitted", true)));

        assertGraphs(tempDir);
        assertModels(tempDir);
    }

    @Test
    void shouldCollectErrorsWhenPersistingGraphStores() throws IOException {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        var first = tempDir.resolve("graphs/first");
        Files.createDirectories(first);

        assertCypherResult(shutdownQuery, List.of(Map.of("submitted", true)));

        assertThat(testLog.getMessages(TestLog.WARN))
            .contains(
                "Persisting graph 'first' for user 'userA' failed - The specified export directory already exists.")
            .contains(BackupAndRestore.BackupResult.failedGraph().toString());

        assertThat(first).isEmptyDirectory();

        assertGraph(tempDir.resolve("graphs/second"));
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
