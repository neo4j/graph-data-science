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
package org.neo4j.gds.beta.k1coloring;

import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableNodeProjection;
import org.neo4j.gds.ImmutableNodeProjections;
import org.neo4j.gds.ImmutablePropertyMappings;
import org.neo4j.gds.InvocationCountingTaskStore;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProcedureMethodHelper;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.utils.StringJoining;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class K1ColoringStatsProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String K1COLORING_GRAPH = "myGraph";

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(b)" +
        ",(c)" +
        ",(d)" +
        ",(a)-[:REL]->(b)" +
        ",(a)-[:REL]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            K1ColoringStatsProc.class,
            GraphProjectProc.class
        );
        runQuery(
            GdsCypher.call(K1COLORING_GRAPH)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStats() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH)
            .algo("gds", "beta", "k1coloring")
            .statsMode()
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("preProcessingMillis").longValue());
            assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(2, row.getNumber("colorCount").longValue());
            assertTrue(row.getBoolean("didConverge"));
            assertTrue(row.getNumber("ranIterations").longValue() < 3);
        });
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();
        TestProcedureRunner.applyOnProcedure(db, K1ColoringStatsProc.class, proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var configMap = Map.<String, Object>of();
            proc.stats(K1COLORING_GRAPH, configMap).count();
            proc.stats(K1COLORING_GRAPH, configMap).count();

            assertThat(taskStore.query())
                .withFailMessage(() -> formatWithLocale(
                    "Expected no tasks to be open but found [%s]",
                    StringJoining.join(taskStore.query().map(TaskStore.UserTask::task).map(Task::description))
                )).isEmpty();
            assertThat(taskStore.registerTaskInvocations).isGreaterThan(1);
        });
    }

    @Test
    void shouldRegisterTaskWithCorrectJobId() {
        var taskStore = new InvocationCountingTaskStore();
        TestProcedureRunner.applyOnProcedure(db, K1ColoringStatsProc.class, proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);
            var someJobId = new JobId();
            Map<String, Object> configMap = Map.of("jobId", someJobId);
            proc.stats(K1COLORING_GRAPH, configMap);
            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    @Test
    void testRunOnEmptyGraph() {
        // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
        TestProcedureRunner.applyOnProcedure(db, K1ColoringStatsProc.class, (proc) -> {
            var methods = ProcedureMethodHelper.statsMethods(proc).collect(Collectors.toList());
            if (!methods.isEmpty()) {
                // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
                runQuery("CALL db.createLabel('X')");
                runQuery("MATCH (n) DETACH DELETE n");
                GraphStoreCatalog.removeAllLoadedGraphs();

                var graphName = "graph";
                var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
                    TEST_USERNAME,
                    graphName,
                    ImmutableNodeProjections.of(
                        Map.of(NodeLabel.of("X"), ImmutableNodeProjection.of("X", ImmutablePropertyMappings.of()))
                    ),
                    RelationshipProjections.ALL
                );
                var graphStore = graphLoader(graphProjectConfig).graphStore();
                GraphStoreCatalog.set(graphProjectConfig, graphStore);
                methods.forEach(method -> {
                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, graphName, Map.<String, Object>of());
                        assertEquals(1, result.count());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
            }
        });
    }

    @NotNull
    private GraphLoader graphLoader(GraphProjectConfig graphProjectConfig) {
        return ImmutableGraphLoader
            .builder()
            .context(ImmutableGraphLoaderContext.builder()
                .databaseId(DatabaseId.of(db))
                .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(db))
                .transactionContext(TestSupport.fullAccessTransaction(db))
                .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
                .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
                .log(Neo4jProxy.testLog())
                .build())
            .username("")
            .projectConfig(graphProjectConfig)
            .build();
    }
}
