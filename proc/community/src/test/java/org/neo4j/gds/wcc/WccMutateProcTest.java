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
package org.neo4j.gds.wcc;

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
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.utils.StringJoining;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class WccMutateProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String MUTATE_PROPERTY = "componentId";
    private static final String GRAPH_NAME = "loadGraph";
    private static final String EXPECTED_MUTATED_GRAPH = "  (a {componentId: 0})" +
                                                         ", (b {componentId: 0})" +
                                                         ", (c {componentId: 0})" +
                                                         ", (d {componentId: 0})" +
                                                         ", (e {componentId: 0})" +
                                                         ", (f {componentId: 0})" +
                                                         ", (g {componentId: 0})" +
                                                         ", (h {componentId: 7})" +
                                                         ", (i {componentId: 7})" +
                                                         ", (j {componentId: 9})" +
                                                         // {A, B, C, D}
                                                         ", (a)-[{w: 1.0d}]->(b)" +
                                                         ", (b)-[{w: 1.0d}]->(c)" +
                                                         ", (c)-[{w: 1.0d}]->(d)" +
                                                         ", (d)-[{w: 1.0d}]->(e)" +
                                                         // {E, F, G}
                                                         ", (e)-[{w: 1.0d}]->(f)" +
                                                         ", (f)-[{w: 1.0d}]->(g)" +
                                                         // {H, I}
                                                         ", (h)-[{w: 1.0d}]->(i)";
    @Neo4jGraph
    static final @Language("Cypher") String DB_CYPHER =
        "CREATE" +
        " (nA:Label {nodeId: 0, seedId: 42})" +
        ",(nB:Label {nodeId: 1, seedId: 42})" +
        ",(nC:Label {nodeId: 2, seedId: 42})" +
        ",(nD:Label {nodeId: 3, seedId: 42})" +
        ",(nE:Label2 {nodeId: 4})" +
        ",(nF:Label2 {nodeId: 5})" +
        ",(nG:Label2 {nodeId: 6})" +
        ",(nH:Label2 {nodeId: 7})" +
        ",(nI:Label2 {nodeId: 8})" +
        ",(nJ:Label2 {nodeId: 9})" +
        // {A, B, C, D}
        ",(nA)-[:TYPE]->(nB)" +
        ",(nB)-[:TYPE]->(nC)" +
        ",(nC)-[:TYPE]->(nD)" +
        ",(nD)-[:TYPE {cost:4.2}]->(nE)" + // threshold UF should split here
        // {E, F, G}
        ",(nE)-[:TYPE]->(nF)" +
        ",(nF)-[:TYPE]->(nG)" +
        // {H, I}
        ",(nH)-[:TYPE]->(nI)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            WccMutateProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testMutateAndWriteWithSeeding() throws Exception {
        registerProcedures(WccWriteProc.class);
        var testGraphName = "wccGraph";
        var initialGraphStore = new StoreLoaderBuilder().databaseService(db)
            .build()
            .graphStore();

        GraphStoreCatalog.set(
            withNameAndRelationshipProjections(testGraphName, RelationshipProjections.ALL),
            initialGraphStore
        );

        var mutateQuery = GdsCypher
            .call(testGraphName)
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(mutateQuery);

        var writeQuery = GdsCypher
            .call(testGraphName)
            .algo("wcc")
            .writeMode()
            .addParameter("seedProperty", MUTATE_PROPERTY)
            .addParameter("writeProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(writeQuery);

        var updatedGraph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty(MUTATE_PROPERTY, MUTATE_PROPERTY, DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(fromGdl(EXPECTED_MUTATED_GRAPH), updatedGraph);
    }

    @Test
    void testMutateYields() {
        var initialGraphStore = new StoreLoaderBuilder().databaseService(db)
            .build()
            .graphStore();

        GraphStoreCatalog.set(
            withNameAndRelationshipProjections(GRAPH_NAME, RelationshipProjections.ALL),
            initialGraphStore
        );

        String query = GdsCypher
            .call(GRAPH_NAME)
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields(
                "nodePropertiesWritten",
                "preProcessingMillis",
                "computeMillis",
                "mutateMillis",
                "postProcessingMillis",
                "componentCount",
                "componentDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(10L);

                assertThat(row.getNumber("preProcessingMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("computeMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("postProcessingMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("mutateMillis"))
                    .asInstanceOf(LONG)
                    .isGreaterThan(-1L);

                assertThat(row.getNumber("componentCount"))
                    .asInstanceOf(LONG)
                    .as("wrong component count")
                    .isEqualTo(3L);

                assertUserInput(row, "threshold", 0D);
                assertUserInput(row, "consecutiveIds", false);

                assertThat(row.get("componentDistribution"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(MAP)
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                            "p99", 7L,
                            "min", 1L,
                            "max", 7L,
                            "mean", 3.3333333333333335D,
                            "p999", 7L,
                            "p95", 7L,
                            "p90", 7L,
                            "p75", 7L,
                            "p50", 2L
                        )
                    );
            }
        );
    }

    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");

        String graphName = "emptyGraph";
        var loadQuery = GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();
        runQuery(loadQuery);

        String query = GdsCypher
            .call(graphName)
            .algo("wcc")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .yields("componentCount");

        runQueryWithRowConsumer(query, row -> assertThat(row.getNumber("componentCount"))
            .asInstanceOf(LONG)
            .isEqualTo(0));
    }

    @Test
    void testWriteBackGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (:B), (a1)-[:REL1]->(a2), (a2)-[:REL2]->(b)");
        String graphName = "myGraph";

        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder()
            .databaseService(db)
            .graphName(graphName)
            .addNodeProjection(ImmutableNodeProjection.of(
                "A",
                PropertyMappings.of()
            ))
            .addNodeProjection(ImmutableNodeProjection.of(
                "B",
                PropertyMappings.of()
            ));
        RelationshipProjections.ALL.projections().forEach((relationshipType, projection) ->
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(relationshipType.name(), projection));
        GraphLoader loader = storeLoaderBuilder.build();
        GraphStoreCatalog.set(loader.projectConfig(), loader.graphStore());

        applyOnProcedure(procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    var config = Map.of(
                        "nodeLabels", List.of("B"),
                        "mutateProperty", MUTATE_PROPERTY
                    );
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
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

        runQuery(graphWriteQuery, Map.of("graph", graphName, "property", MUTATE_PROPERTY));

        String checkNeo4jGraphNegativeQuery = formatWithLocale("MATCH (n:A) RETURN n.%s AS property", MUTATE_PROPERTY);

        runQueryWithRowConsumer(
            checkNeo4jGraphNegativeQuery,
            ((transaction, resultRow) -> assertNull(resultRow.get("property")))
        );

        String checkNeo4jGraphPositiveQuery = formatWithLocale("MATCH (n:B) RETURN n.%s AS property", MUTATE_PROPERTY);

        runQueryWithRowConsumer(
            checkNeo4jGraphPositiveQuery,
            ((transaction, resultRow) -> assertNotNull(resultRow.get("property")))
        );
    }

    @Test
    void testGraphMutation() {
        GraphStore graphStore = runMutation(ensureGraphExists(), Map.of());
        assertGraphEquals(fromGdl(EXPECTED_MUTATED_GRAPH), graphStore.getUnion());
        GraphSchema schema = graphStore.schema();
        boolean nodesContainMutateProperty = schema.nodeSchema().entries().stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .anyMatch(props -> props.getKey().equals(MUTATE_PROPERTY) && props.getValue().valueType() == ValueType.LONG);
        assertTrue(nodesContainMutateProperty);
    }

    @Test
    void testGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (a1)-[:REL]->(a2)");
        var relationshipProjections = RelationshipProjections.ALL;
        var orientation = Orientation.NATURAL;
        GraphStore graphStore = new TestNativeGraphLoader(db)
            .withLabels("A", "B")
            .withNodeProperties(ImmutablePropertyMappings.of())
            .withDefaultOrientation(orientation)
            .graphStore();

        String graphName = "myGraph";
        var graphProjectConfig = withNameAndRelationshipProjections(
            graphName,
            relationshipProjections
        );
        GraphStoreCatalog.set(graphProjectConfig, graphStore);

        var filterConfig = Map.<String, Object>of(
            "nodeLabels",
            Collections.singletonList("A")
        );

        runMutation(graphName, filterConfig);

        GraphStore mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), graphName).graphStore();

        var expectedProperties = new ArrayList<String>();
        expectedProperties.add(MUTATE_PROPERTY);
        assertEquals(new HashSet<>(expectedProperties), mutatedGraph.nodePropertyKeys(NodeLabel.of("A")));
        assertEquals(new HashSet<>(), mutatedGraph.nodePropertyKeys(NodeLabel.of("B")));
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
        assertGraphEquals(fromGdl(EXPECTED_MUTATED_GRAPH), mutatedGraph);
    }

    @Test
    void testExceptionLogging() {
        List<TestLog> log = new ArrayList<>(1);
        assertThrows(
            NullPointerException.class,
            () -> applyOnProcedure(procedure -> {
                var computationResult = mock(ComputationResult.class);
                log.add(0, ((TestLog) procedure.log));
                new WccMutateSpecification().computationResultConsumer().consume(computationResult, procedure.executionContext());
            })
        );

        assertTrue(log.get(0).containsMessage(TestLog.WARN, "Graph mutation failed"));
    }

    @Test
    void shouldUnregisterTaskAfterComputation() {
        var taskStore = new InvocationCountingTaskStore();
        var graphProjectConfig = withNameAndRelationshipProjections(
            GRAPH_NAME,
            RelationshipProjections.ALL
        );
        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
        applyOnProcedure(wccMutateProc -> {
            wccMutateProc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            var configMap = Map.<String, Object>of("mutateProperty", MUTATE_PROPERTY);

            var spec = new WccMutateSpecification() {
                @Override
                public ComputationResultConsumer<Wcc, DisjointSetStruct, WccMutateConfig, Stream<MutateResult>> computationResultConsumer() {
                    return (computationResultConsumer, executionContext) -> {
                        computationResultConsumer.result().get();
                        return Stream.empty();
                    };
                }
            };
            new ProcedureExecutor<>(spec, wccMutateProc.executionContext()).compute(GRAPH_NAME, configMap);
            new ProcedureExecutor<>(spec, wccMutateProc.executionContext()).compute(GRAPH_NAME, configMap);

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

        GraphProjectConfig graphProjectConfig = withNameAndRelationshipProjections(
            GRAPH_NAME,
            RelationshipProjections.ALL
        );
        applyOnProcedure(proc -> {
            proc.taskRegistryFactory = jobId -> new TaskRegistry("", taskStore, jobId);

            GraphStore graphStore = graphLoader(graphProjectConfig).graphStore();
            GraphStoreCatalog.set(graphProjectConfig, graphStore);

            var someJobId = new JobId();
            Map<String, Object> configMap = Map.of(
                "jobId", someJobId,
                "mutateProperty", MUTATE_PROPERTY
            );

            proc.mutate(GRAPH_NAME, configMap);

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

                var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
                    TEST_USERNAME,
                    GRAPH_NAME,
                    ImmutableNodeProjections.of(
                        Map.of(NodeLabel.of("X"), ImmutableNodeProjection.of("X", ImmutablePropertyMappings.of()))
                    ),
                    RelationshipProjections.ALL
                );
                GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
                methods.forEach(method -> {
                    Map<String, Object> configMap = Map.of("mutateProperty", MUTATE_PROPERTY);
                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, GRAPH_NAME, configMap);
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
        GraphProjectConfig graphProjectConfig = withNameAndRelationshipProjections(
            GRAPH_NAME,
            RelationshipProjections.ALL
        );
        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
        return GRAPH_NAME;
    }

    @NotNull
    private GraphStore runMutation(String graphName, Map<String, Object> additionalConfig) {
        applyOnProcedure(procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    Map<String, Object> config = new HashMap<>(additionalConfig);
                    config.put("mutateProperty", MUTATE_PROPERTY);
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        return GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), graphName).graphStore();
    }

    void applyOnProcedure(Consumer<WccMutateProc> func) {
        TestProcedureRunner.applyOnProcedure(
            db,
            WccMutateProc.class,
            func
        );
    }

    private GraphProjectFromStoreConfig withNameAndRelationshipProjections(
        String graphName,
        RelationshipProjections rels
    ) {
        return ImmutableGraphProjectFromStoreConfig.of(
            TEST_USERNAME,
            graphName,
            NodeProjections.create(singletonMap(
                ALL_NODES,
                ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of())
            )),
            rels
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
