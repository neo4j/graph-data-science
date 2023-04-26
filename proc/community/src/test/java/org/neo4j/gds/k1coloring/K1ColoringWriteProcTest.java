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
package org.neo4j.gds.k1coloring;

import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.utils.StringJoining;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class K1ColoringWriteProcTest extends BaseProcTest {

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
            K1ColoringWriteProc.class,
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
    void testWriting() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
            .writeMode()
            .addParameter("writeProperty", "color")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("preProcessingMillis").longValue());
            assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
            assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(2, row.getNumber("colorCount").longValue());
            assertUserInput(row, "writeProperty", "color");
            assertTrue(row.getBoolean("didConverge"));
            assertTrue(row.getNumber("ranIterations").longValue() < 3);
        });

        Map<Long, Long> coloringResult = new HashMap<>(4);
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.color AS color", row -> {
            long nodeId = row.getNumber("id").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(nodeId, color);
        });

        assertNotEquals(coloringResult.get(0L), coloringResult.get(1L));
        assertNotEquals(coloringResult.get(0L), coloringResult.get(2L));
    }

    @Test
    void testWritingEstimate() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
            .estimationMode(GdsCypher.ExecutionModes.WRITE)
            .addParameter("writeProperty", "color")
            .yields("requiredMemory", "treeView", "bytesMin", "bytesMax");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);

            String bytesHuman = MemoryUsage.humanReadable(row.getNumber("bytesMin").longValue());
            assertNotNull(bytesHuman);
            assertTrue(row.getString("requiredMemory").contains(bytesHuman));
        });
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), Map.of(
                        0L, 1L,
                        1L, 0L,
                        2L, 0L,
                        3L, 0L
                )),
                Arguments.of(Map.of("minCommunitySize", 2), Map.of(
                        1L, 0L,
                        2L, 0L,
                        3L, 0L
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Long> parameter, Map<Long, Long> expectedResult) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
                .writeMode()
                .addParameter("writeProperty", "color")
                .addAllParameters(parameter)
                .yields("nodeCount", "colorCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(4L, row.getNumber("nodeCount"));
            assertEquals(2L, row.getNumber("colorCount"));
        });

        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.color AS color", row -> {
            long nodeId = row.getNumber("id").longValue();
            assertEquals(expectedResult.get(nodeId), row.getNumber("color"));
        });
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();

        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            Map<String, Object> configMap = Map.of("writeProperty", "color");
            var spec = new K1ColoringWriteSpecification() {
                @Override
                public ComputationResultConsumer<K1Coloring, HugeLongArray, K1ColoringWriteConfig, Stream<K1ColoringWriteResult>> computationResultConsumer() {
                    return (computationResult, executionContext) -> {
                        computationResult.result().get();
                        return Stream.empty();
                    };
                }
            };
            new ProcedureExecutor<>(spec, proc.executionContext()).compute(K1COLORING_GRAPH, configMap).count();
            new ProcedureExecutor<>(spec, proc.executionContext()).compute(K1COLORING_GRAPH, configMap).count();

            assertThat(taskStore.query())
                .withFailMessage(() -> formatWithLocale(
                    "Expected no tasks to be open but found %s",
                    StringJoining.join(taskStore.query().map(TaskStore.UserTask::task).map(Task::description))
                )).isEmpty();
            assertThat(taskStore.registerTaskInvocations).isGreaterThan(1);
        });
    }

    @Test
    void shouldRegisterTaskWithCorrectJobId() {
        var taskStore = new InvocationCountingTaskStore();

        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var someJobId = new JobId();
            Map<String, Object> configMap = Map.of(
                "jobId", someJobId,
                "writeProperty", "color"
            );
            var spec = new K1ColoringWriteSpecification() {
                @Override
                public ComputationResultConsumer<K1Coloring, HugeLongArray, K1ColoringWriteConfig, Stream<K1ColoringWriteResult>> computationResultConsumer() {
                    return (computationResult, executionContext) -> {
                        computationResult.result().get();
                        return Stream.empty();
                    };
                }
            };
            new ProcedureExecutor<>(spec, proc.executionContext()).compute(K1COLORING_GRAPH, configMap);

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    @Test
    void testRunOnEmptyGraph() {
        applyOnProcedure((proc) -> {
            var methods = ProcedureMethodHelper.writeMethods(proc).collect(Collectors.toList());

            if (!methods.isEmpty()) {
                // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
                runQuery("CALL db.createLabel('X')");
                runQuery("MATCH (n) DETACH DELETE n");
                GraphStoreCatalog.removeAllLoadedGraphs();

                var graphName = "graph";
                var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
                    Username.EMPTY_USERNAME.username(),
                    graphName,
                    ImmutableNodeProjections.of(
                        Map.of(NodeLabel.of("X"), ImmutableNodeProjection.of("X", ImmutablePropertyMappings.of()))
                    ),
                    RelationshipProjections.ALL
                );
                var graphStore = graphLoader(graphProjectConfig).graphStore();
                GraphStoreCatalog.set(graphProjectConfig, graphStore);
                methods.forEach(method -> {
                    Map<String, Object> configMap = Map.of("writeProperty", "color");
                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, graphName, configMap);
                        assertEquals(1, result.count());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
            }
        });
    }

    private void applyOnProcedure(Consumer<K1ColoringWriteProc> func) {
        TestProcedureRunner.applyOnProcedure(
            db,
            K1ColoringWriteProc.class,
            proc -> {
                proc.nodePropertyExporterBuilder = new NativeNodePropertiesExporterBuilder(
                    DatabaseTransactionContext.of(proc.databaseService, proc.procedureTransaction)
                );
                func.accept(proc);
            }
        );
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
