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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    GraphDatabaseService graphDb();

    CONFIG createConfig(CypherMapWrapper mapWrapper);

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

    default void consumeResult(RESULT result) {}

    @Test
    default void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();

        var loadedGraphName = "loadedGraph";
        var graphProjectConfig = withNameAndRelationshipProjections(
            loadedGraphName,
            relationshipProjections()
        );

        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());

        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
            consumeResult(
                proc.compute(
                    loadedGraphName,
                    configMap
                ).result().get()
            );
            consumeResult(
                proc.compute(
                    loadedGraphName,
                    configMap
                ).result().get()
            );

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
            loadedGraphName,
            relationshipProjections()
        );
        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
            GraphStoreCatalog.set(graphProjectConfig, graphStore);

            var someJobId = new JobId();
            Map<String, Object> mapWithJobId = Map.of("jobId", someJobId);

            Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.create(mapWithJobId)).toMap();
            proc.compute(loadedGraphName, configMap);

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    default RelationshipProjections relationshipProjections() {
        return RelationshipProjections.ALL;
    }

    @Test
    default void testRunOnEmptyGraph() {
        applyOnProcedure((proc) -> {
            var methods = Stream.concat(
                ProcedureMethodHelper.writeMethods(proc),
                ProcedureMethodHelper.streamMethods(proc)
            ).collect(Collectors.toList());

            if (!methods.isEmpty()) {
                // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
                runQuery(graphDb(), "CALL db.createLabel('X')");
                runQuery(graphDb(), "MATCH (n) DETACH DELETE n");
                GraphStoreCatalog.removeAllLoadedGraphs();

                var graphName = "graph";
                var graphProjectConfig = withNameAndProjections(
                    graphName,
                    ImmutableNodeProjections.of(
                        Map.of(NodeLabel.of("X"), ImmutableNodeProjection.of("X", ImmutablePropertyMappings.of()))
                    ),
                    relationshipProjections()
                );
                var graphStore = graphLoader(graphProjectConfig).graphStore();
                GraphStoreCatalog.set(graphProjectConfig, graphStore);
                methods.forEach(method -> {
                    Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();

                    configMap.remove(NODE_PROPERTIES_KEY);
                    configMap.remove(RELATIONSHIP_PROPERTIES_KEY);
                    configMap.remove("relationshipWeightProperty");

                    if (configMap.containsKey("nodeWeightProperty")) {
                        var nodeProperty = String.valueOf(configMap.get("nodeWeightProperty"));
                        runQuery(
                            graphDb(),
                            "CALL db.createProperty($prop)",
                            Map.of("prop", nodeProperty)
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
                        Stream<?> result = (Stream<?>) method.invoke(proc, graphName, configMap);
                        if (ProcedureMethodHelper.methodName(method).endsWith("stream")) {
                            assertEquals(0, result.count(), "Stream result should be empty.");
                        } else {
                            assertEquals(1, result.count());
                        }
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
            }
        });
    }

    default void loadGraph(String graphName) {
        runQuery(
            graphDb(),
            GdsCypher.call(graphName)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @NotNull
    default GraphLoader graphLoader(GraphProjectConfig graphProjectConfig) {
        GraphDatabaseService db = graphDb();
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
