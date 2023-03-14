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
package org.neo4j.gds;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.core.write.NativeRelationshipExporterBuilder;
import org.neo4j.gds.core.write.NativeRelationshipStreamExporterBuilder;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.NODE_PROPERTIES_KEY;
import static org.neo4j.gds.config.GraphProjectFromStoreConfig.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * @deprecated do not add new uses of this.
 * Instead, copy any test cases you might want to use into your specific procedure test classes.
 */
@ApiStatus.Obsolete
public interface AlgoBaseProcTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends AlgoBaseConfig, RESULT>
    extends GraphProjectConfigSupport {

    String TEST_USERNAME = Username.EMPTY_USERNAME.username();

    @AfterEach
    default void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    Class<? extends AlgoBaseProc<ALGORITHM, RESULT, CONFIG, ?>> getProcedureClazz();

    default AlgoBaseProc<ALGORITHM, RESULT, CONFIG, ?> proc() {
        try {
            return getProcedureClazz()
                .getConstructor()
                .newInstance();
        } catch (Exception e) {
            fail("unable to instantiate procedure", e);
        }
        return null;
    }

    GraphDatabaseService graphDb();

    default DatabaseId databaseId() {
        return DatabaseId.of(graphDb());
    }

    default GraphDatabaseService emptyDb() {
        var db = graphDb();
        runQuery(db, "MATCH (n) DETACH DELETE n");
        return db;
    }

    CONFIG createConfig(CypherMapWrapper mapWrapper);

    void assertResultEquals(RESULT result1, RESULT result2);

    default CypherMapWrapper createMinimalConfig() {
        return createMinimalConfig(CypherMapWrapper.empty());
    }

    default CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper;
    }

    default void applyOnProcedure(Consumer<? super AlgoBaseProc<ALGORITHM, RESULT, CONFIG, ?>> func) {
        TestProcedureRunner.applyOnProcedure(
            graphDb(),
            getProcedureClazz(),
            proc -> {
                if (proc instanceof NodePropertiesWriter) {
                    ((NodePropertiesWriter<?, ?, ?, ?>) proc).nodePropertyExporterBuilder = new NativeNodePropertiesExporterBuilder(
                        DatabaseTransactionContext.of(
                            proc.databaseService,
                            proc.procedureTransaction
                        ));
                }

                if (proc instanceof WriteRelationshipsProc) {
                    ((WriteRelationshipsProc<?, ?, ?, ?>) proc).relationshipExporterBuilder = new NativeRelationshipExporterBuilder(
                        DatabaseTransactionContext.of(
                            proc.databaseService,
                            proc.procedureTransaction
                        ));
                }

                if (proc instanceof StreamOfRelationshipsWriter) {
                    ((StreamOfRelationshipsWriter<?, ?, ?, ?>) proc).relationshipStreamExporterBuilder = new NativeRelationshipStreamExporterBuilder(
                        DatabaseTransactionContext.of(
                            proc.databaseService,
                            proc.procedureTransaction
                        ));
                }

                func.accept(proc);
            }
        );
    }

    default void assertMissingProperty(String error, Runnable runnable) {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            runnable::run
        );
        assertThat(exception).hasMessageContaining(error);
    }

    class InvocationCountingTaskStore extends GlobalTaskStore {
        public int registerTaskInvocations;
        public int removeTaskInvocations;
        Set<JobId> seenJobIds = new HashSet<>();

        @Override
        public void store(
            String username, JobId jobId, Task task
        ) {
            super.store(username, jobId, task);
            registerTaskInvocations++;

            seenJobIds.add(jobId);
        }

        @Override
        public void remove(String username, JobId jobId) {
            super.remove(username, jobId);
            removeTaskInvocations++;
        }
    }

    @Test
    default void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();

        var loadedGraphName = "loadedGraph";
        var graphProjectConfig = withNameAndRelationshipProjections(
            "",
            loadedGraphName,
            relationshipProjections(),
            nodeProperties()
        );

        GraphStoreCatalog.set(
            graphProjectConfig,
            graphLoader(graphProjectConfig).graphStore()
        );

        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            runTwiceAndAssertEqualResult(loadedGraphName, proc);

            assertThat(taskStore.query())
                .withFailMessage(() -> formatWithLocale(
                    "Expected no tasks to be open but found %s",
                    StringJoining.join(taskStore.query().map(TaskStore.UserTask::task).map(Task::description))
                )).isEmpty();
            assertThat(taskStore.registerTaskInvocations).isGreaterThan(1);
        });
    }

    @Test
    default void shouldRegisterTaskWithCorrectJobId() {
        var taskStore = new InvocationCountingTaskStore();

        String loadedGraphName = "loadedGraph";
        GraphProjectConfig graphProjectConfig = withNameAndRelationshipProjections(
            "",
            loadedGraphName,
            relationshipProjections(),
            nodeProperties()
        );
        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
            GraphStoreCatalog.set(
                graphProjectConfig,
                graphStore
            );

            var someJobId = new JobId();
            Map<String, Object> mapWithJobId = Map.of("jobId", someJobId);

            Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.create(mapWithJobId)).toMap();
            proc.compute(
                loadedGraphName,
                configMap
            );

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    default RelationshipProjections relationshipProjections() {
        return RelationshipProjections.ALL;
    }

    default boolean requiresUndirected() {
        return false;
    }

    default List<String> nodeProperties() {
        return List.of();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    default void testRunMultipleTimesOnLoadedGraph(boolean cypherProjection) {
        var loadedGraphName = "loadedGraph";
        var graphProjectConfig = cypherProjection
            ? emptyWithNameCypher(TEST_USERNAME, loadedGraphName, nodeProperties())
            : withNameAndRelationshipProjections(TEST_USERNAME, loadedGraphName, relationshipProjections(),
                nodeProperties()
            );

        GraphStoreCatalog.set(
            graphProjectConfig,
            graphLoader(graphProjectConfig).graphStore()
        );

        applyOnProcedure(proc -> runTwiceAndAssertEqualResult(loadedGraphName, proc));
    }

    private void runTwiceAndAssertEqualResult(String loadedGraphName, AlgoBaseProc<ALGORITHM, RESULT, CONFIG, ?> proc) {
        var configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
        var resultRun1 = proc.compute(
            loadedGraphName,
            configMap
        );
        var resultRun2 = proc.compute(
            loadedGraphName,
            configMap
        );

        assertResultEquals(resultRun1.result(), resultRun2.result());
    }

    @Test
    default void testRunOnEmptyGraph() {
        // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
        runQuery(graphDb(), "CREATE (n: X)");
        runQuery(graphDb(), "MATCH (n) DETACH DELETE n");

        applyOnProcedure((proc) -> {
            GraphStoreCatalog.removeAllLoadedGraphs();
            var loadedGraphName = "graph";
            var propertyMappings = nodeProperties().stream().map(prop -> PropertyMapping.of(prop, 1.0)).collect(Collectors.toList());
            GraphProjectConfig graphProjectConfig = withNameAndProjections(
                "",
                loadedGraphName,
                ImmutableNodeProjections.of(
                    Map.of(
                        NodeLabel.of("X"), ImmutableNodeProjection.of("X", ImmutablePropertyMappings.of(propertyMappings))
                    )
                ),
                relationshipProjections()
            );
            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
            GraphStoreCatalog.set(graphProjectConfig, graphStore);
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();

                    configMap.remove(NODE_PROPERTIES_KEY);
                    configMap.remove(RELATIONSHIP_PROPERTIES_KEY);
                    configMap.remove("relationshipWeightProperty");

                    var nodeWeightProperty = configMap.get("nodeWeightProperty");
                    if (nodeWeightProperty != null) {
                        var nodeProperty = String.valueOf(nodeWeightProperty);
                        runQuery(
                            graphDb(),
                            "CALL db.createProperty($prop)",
                            Map.of("prop", nodeWeightProperty)
                        );
                        configMap.put(NODE_PROPERTIES_KEY, Map.ofEntries(ImmutablePropertyMapping
                            .builder()
                            .propertyKey(nodeProperty)
                            .defaultValue(DefaultValue.forDouble())
                            .build()
                            .toObject(false)
                        ));
                    }

                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, loadedGraphName, configMap);

                        if (getProcedureMethodName(method).endsWith("stream")) {
                            assertEquals(0, result.count(), "Stream result should be empty.");
                        } else {
                            assertEquals(1, result.count());
                        }

                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
        });
    }

    default void loadGraph(String graphName) {
        loadGraph(graphName, Orientation.NATURAL);
    }

    default void loadGraph(String graphName, Orientation orientation) {
        runQuery(
            graphDb(),
            GdsCypher.call(graphName)
                .graphProject()
                .loadEverything(orientation)
                .withNodeProperties(nodeProperties(), DefaultValue.DEFAULT)
                .yields()
        );
    }

    @Test
    default void checkStatsModeExists() {
        applyOnProcedure((proc) -> {
            boolean inStatsClass = methodExists(proc, "stats");
            if (inStatsClass) {
                assertTrue(
                    methodExists(proc, "stats"),
                    formatWithLocale("Expected %s to have a `stats` method", proc.getClass().getSimpleName())
                );
            }
        });
    }

    default boolean methodExists(AlgoBaseProc<?, RESULT, CONFIG, ?> proc, String methodSuffix) {
        return getProcedureMethods(proc)
            .anyMatch(method -> getProcedureMethodName(method).endsWith(methodSuffix));
    }

    default Stream<Method> getProcedureMethods(AlgoBaseProc<?, RESULT, CONFIG, ?> proc) {
        return Arrays.stream(proc.getClass().getDeclaredMethods())
            .filter(method -> method.getDeclaredAnnotation(Procedure.class) != null);
    }

    default String getProcedureMethodName(Method method) {
        Procedure procedureAnnotation = method.getDeclaredAnnotation(Procedure.class);
        Objects.requireNonNull(procedureAnnotation, method + " is not annotation with " + Procedure.class);
        String name = procedureAnnotation.name();
        if (name.isEmpty()) {
            name = procedureAnnotation.value();
        }
        return name;
    }

    default Stream<Method> getWriteAndStreamProcedures(AlgoBaseProc<?, RESULT, CONFIG, ?> proc) {
        return getProcedureMethods(proc)
            .filter(method -> {
                String procedureMethodName = getProcedureMethodName(method);
                return procedureMethodName.endsWith("stream") || procedureMethodName.endsWith("write");
            });
    }

    @NotNull
    default GraphLoader graphLoader(GraphProjectConfig graphProjectConfig) {
        return graphLoader(graphDb(), graphProjectConfig);
    }

    @NotNull
    default GraphLoader graphLoader(
        GraphDatabaseService db,
        GraphProjectConfig graphProjectConfig
    ) {
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
