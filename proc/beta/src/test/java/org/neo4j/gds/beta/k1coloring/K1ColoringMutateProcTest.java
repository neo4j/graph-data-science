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
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProcedureMethodHelper;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestNativeGraphLoader;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
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
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.utils.StringJoining;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class K1ColoringMutateProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String K1COLORING_GRAPH = "myGraph";
    private static final String MUTATE_PROPERTY = "color";

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
            K1ColoringMutateProc.class,
            GraphWriteNodePropertiesProc.class,
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

    private String expectedMutatedGraph() {
        return
            "  (x { color: 0 }) " +
            ", (y { color: 0 }) " +
            ", (z { color: 0 }) " +
            ", (w { color: 1 })-->(y) " +
            ", (w)-->(z) ";
    }

    @Test
    void testMutateYields() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(4, row.getNumber("nodeCount").longValue(), "wrong nodeCount");
            assertTrue(row.getBoolean("didConverge"), "did not converge");
            assertTrue(row.getNumber("ranIterations").longValue() < 3, "wrong ranIterations");
            assertEquals(2, row.getNumber("colorCount").longValue(), "wrong color count");
            assertTrue(row.getNumber("preProcessingMillis").longValue() >= 0, "invalid preProcessingMillis");
            assertTrue(row.getNumber("mutateMillis").longValue() >= 0, "invalid mutateMillis");
            assertTrue(row.getNumber("computeMillis").longValue() >= 0, "invalid computeMillis");
        });
    }

    @Test
    void testMutateEstimate() {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "beta", "k1coloring")
            .mutateEstimation()
            .addParameter("mutateProperty", "color")
            .yields("nodeCount", "bytesMin", "bytesMax", "requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 4L,
            "bytesMin", 552L,
            "bytesMax", 552L,
            "requiredMemory", "552 Bytes"
        )));
    }

    @Test
    void testWriteBackGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (:B), (a1)-[:REL1]->(a2), (a2)-[:REL2]->(b)");

        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder()
            .databaseService(db)
            .graphName(K1COLORING_GRAPH)
            .addNodeProjection(ImmutableNodeProjection.of("A", PropertyMappings.of()))
            .addNodeProjection(ImmutableNodeProjection.of("B", PropertyMappings.of()));
        RelationshipProjections.ALL.projections().forEach((relationshipType, projection) ->
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(relationshipType.name(), projection));
        GraphLoader loader = storeLoaderBuilder.build();

        GraphStoreCatalog.set(loader.projectConfig(), loader.graphStore());

        applyOnProcedure(procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    Map<String, Object> config = Map.of(
                        "nodeLabels", Collections.singletonList("B"),
                        "mutateProperty", MUTATE_PROPERTY
                    );
                    try {
                        mutateMethod.invoke(procedure, K1COLORING_GRAPH, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        String graphWriteQuery =
            "CALL gds.graph.nodeProperties.write(" +
            "   $graph, " +
            "   [$property]" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten";

        runQuery(graphWriteQuery, Map.of("graph", K1COLORING_GRAPH, "property", MUTATE_PROPERTY));

        String checkNeo4jGraphNegativeQuery = formatWithLocale("MATCH (n:A) RETURN n.%s AS property", MUTATE_PROPERTY);

        runQueryWithRowConsumer(
            db,
            checkNeo4jGraphNegativeQuery,
            Map.of(),
            (resultRow) -> assertNull(resultRow.get("property"))
        );

        String checkNeo4jGraphPositiveQuery = formatWithLocale("MATCH (n:B) RETURN n.%s AS property", MUTATE_PROPERTY);

        runQueryWithRowConsumer(
            db,
            checkNeo4jGraphPositiveQuery,
            Map.of(),
            (resultRow) -> assertNotNull(resultRow.get("property"))
        );
    }

    @Test
    void testGraphMutation() {
        GraphStore graphStore = runMutation(ensureGraphExists(), Map.of("mutateProperty", MUTATE_PROPERTY));
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());
        var containsMutateProperty =  graphStore.schema().nodeSchema()
            .entries()
            .stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .anyMatch(
                props -> props.getKey().equals(MUTATE_PROPERTY) &&
                         props.getValue().valueType() == ValueType.LONG
            );
        assertThat(containsMutateProperty).isTrue();
    }

    @Test
    void testGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (a1)-[:REL]->(a2)");
        var graphStore = new TestNativeGraphLoader(db)
            .withLabels("A", "B")
            .withNodeProperties(ImmutablePropertyMappings.of())
            .withDefaultOrientation(Orientation.NATURAL)
            .graphStore();

        var graphProjectConfig = withAllNodesAndRelationshipsProjectConfig(K1COLORING_GRAPH);
        GraphStoreCatalog.set(graphProjectConfig, graphStore);

        Map<String, Object> config = Map.of(
            "nodeLabels", Collections.singletonList("A"),
            "mutateProperty", MUTATE_PROPERTY
        );
        runMutation(K1COLORING_GRAPH, config);

        var mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), K1COLORING_GRAPH).graphStore();

        var expectedProperties = Set.of(MUTATE_PROPERTY);
        assertEquals(expectedProperties, mutatedGraph.nodePropertyKeys(NodeLabel.of("A")));
        assertEquals(Set.of(), mutatedGraph.nodePropertyKeys(NodeLabel.of("B")));
    }

    @Test
    void testMutateFailsOnExistingToken() {
        String graphName = ensureGraphExists();

        applyOnProcedure(procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    Map<String, Object> config = Map.of("mutateProperty", MUTATE_PROPERTY);
                    try {
                        // mutate first time
                        mutateMethod.invoke(procedure, graphName, config);
                        // mutate second time using same `mutateProperty`
                        assertThatThrownBy(() -> mutateMethod.invoke(procedure, graphName, config))
                            .hasRootCauseInstanceOf(IllegalArgumentException.class)
                            .hasRootCauseMessage(formatWithLocale(
                                "Node property `%s` already exists in the in-memory graph.",
                                MUTATE_PROPERTY
                            ));
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), graphName).graphStore().getUnion();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Test
    void testExceptionLogging() {
        List<TestLog> log = new ArrayList<>(1);
        assertThrows(
            NullPointerException.class,
            () -> applyOnProcedure(procedure -> {
                var computationResult = mock(ComputationResult.class);
                log.add(0, ((TestLog) procedure.log));
                procedure.computationResultConsumer().consume(computationResult, procedure.executionContext());
            })
        );

        assertTrue(log.get(0).containsMessage(TestLog.WARN, "Graph mutation failed"));
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();

        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            Map<String, Object> configMap = Map.of("mutateProperty", MUTATE_PROPERTY);
            proc.compute(configMap, K1COLORING_GRAPH).result().get();
            proc.compute(configMap, K1COLORING_GRAPH).result().get();

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
                "mutateProperty", MUTATE_PROPERTY
            );
            proc.compute(configMap, K1COLORING_GRAPH);

            assertThat(taskStore.seenJobIds).containsExactly(someJobId);
        });
    }

    @Test
    void testRunOnEmptyGraph() {
        applyOnProcedure((proc) -> {
            var methods = ProcedureMethodHelper.mutateMethods(proc).collect(Collectors.toList());

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
                    Map<String, Object> configMap = Map.of("mutateProperty", MUTATE_PROPERTY);
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

    @NotNull
    private String ensureGraphExists() {
        String loadedGraphName = "loadGraph";
        GraphProjectConfig graphProjectConfig = withAllNodesAndRelationshipsProjectConfig(loadedGraphName);
        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
        return loadedGraphName;
    }

    @NotNull
    private GraphStore runMutation(String graphName, Map<String, Object> config) {
        applyOnProcedure(procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        return GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), graphName).graphStore();
    }

    private void applyOnProcedure(Consumer<K1ColoringMutateProc> func) {
        TestProcedureRunner.applyOnProcedure(db, K1ColoringMutateProc.class, func);
    }

    private GraphProjectFromStoreConfig withAllNodesAndRelationshipsProjectConfig(String graphName) {
        return ImmutableGraphProjectFromStoreConfig.of(
            TEST_USERNAME,
            graphName,
            NodeProjections.create(Map.of(
                ALL_NODES, ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of())
            )), RelationshipProjections.ALL
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
