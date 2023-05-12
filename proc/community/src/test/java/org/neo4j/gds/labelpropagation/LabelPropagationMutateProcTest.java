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
package org.neo4j.gds.labelpropagation;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableNodeProjection;
import org.neo4j.gds.ImmutableNodeProjections;
import org.neo4j.gds.ImmutablePropertyMappings;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProcedureMethodHelper;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.QueryRunner;
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
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.extension.Neo4jGraph;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
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

public class LabelPropagationMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {id: 0, seed: 42}) " +
        ", (b:B {id: 1, seed: 42}) " +

        ", (a)-[:X]->(:A {id: 2,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 3,  weight: 2.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 4,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 5,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 6,  weight: 8.0, seed: 2}) " +

        ", (b)-[:X]->(:B {id: 7,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 8,  weight: 2.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 9,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 10, weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 11, weight: 8.0, seed: 2})";
    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String MUTATE_PROPERTY = "communityId";
    private static final String TEST_GRAPH_NAME = "myGraph";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            LabelPropagationMutateProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );
        // Create explicit graphs with both projection variants
        runQuery(
            "CALL gds.graph.project(" +
            "   '" + TEST_GRAPH_NAME + "', " +
            "   {" +
            "       A: {label: 'A', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}, " +
            "       B: {label: 'B', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}" +
            "   }, " +
            "   '*'" +
            ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private String expectedMutatedGraph() {
        return
            "  (a { communityId: 2 }) " +
            ", (b { communityId: 7 }) " +
            ", (a)-->({ communityId: 2 }) " +
            ", (a)-->({ communityId: 3 }) " +
            ", (a)-->({ communityId: 4 }) " +
            ", (a)-->({ communityId: 5 }) " +
            ", (a)-->({ communityId: 6 }) " +
            ", (b)-->({ communityId: 7 }) " +
            ", (b)-->({ communityId: 8 }) " +
            ", (b)-->({ communityId: 9 }) " +
            ", (b)-->({ communityId: 10 }) " +
            ", (b)-->({ communityId: 11 })";
    }

    @Test
    void testMutateAndWriteWithSeeding() throws Exception {
        registerProcedures(LabelPropagationWriteProc.class);
        var testGraphName = "lpaGraph";
        var initialGraphStore = new StoreLoaderBuilder().databaseService(db)
            .build()
            .graphStore();

        GraphStoreCatalog.set(
            allNodesAndRelationshipsProjectConfig(testGraphName),
            initialGraphStore
        );

        var mutateQuery = GdsCypher
            .call(testGraphName)
            .algo("labelPropagation")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(mutateQuery);

        var writeQuery = GdsCypher
            .call(testGraphName)
            .algo("labelPropagation")
            .writeMode()
            .addParameter("seedProperty", MUTATE_PROPERTY)
            .addParameter("writeProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(writeQuery);

        var updatedGraph = new StoreLoaderBuilder().databaseService(db)
            .addNodeProperty(MUTATE_PROPERTY, MUTATE_PROPERTY, DefaultValue.of(42.0), Aggregation.NONE)
            .build()
            .graph();

        assertGraphEquals(fromGdl(expectedMutatedGraph()), updatedGraph);
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(
            query,
            row -> {
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

                assertThat(row.getNumber("nodePropertiesWritten"))
                    .asInstanceOf(LONG)
                    .isEqualTo(12L);

                assertThat(row.getNumber("communityCount"))
                    .asInstanceOf(LONG)
                    .isEqualTo(10L);

                assertThat(row.getBoolean("didConverge")).isTrue();

                assertThat(row.get("communityDistribution"))
                    .isNotNull()
                    .isInstanceOf(Map.class)
                    .asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsEntry("p99", 2L)
                    .containsEntry("min", 1L)
                    .containsEntry("max", 2L)
                    .containsEntry("mean", 1.2D)
                    .containsEntry("p90", 2L)
                    .containsEntry("p50", 1L)
                    .containsEntry("p999", 2L)
                    .containsEntry("p95", 2L)
                    .containsEntry("p75", 1L);
            }
        );
    }

    // FIXME: This doesn't belong here.
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
            .algo("labelPropagation")
            .mutateMode()
            .addParameter("mutateProperty", "foo")
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }

    @Test
    void testWriteBackGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (:B), (a1)-[:REL1]->(a2), (a2)-[:REL2]->(b)");

        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder()
            .databaseService(db)
            .graphName(TEST_GRAPH_NAME)
            .addNodeProjection(ImmutableNodeProjection.of("A", PropertyMappings.of()))
            .addNodeProjection(ImmutableNodeProjection.of("B", PropertyMappings.of()));
        RelationshipProjections.ALL.projections().forEach((relationshipType, projection) ->
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(relationshipType.name(), projection));
        GraphLoader loader = storeLoaderBuilder.build();
        GraphStoreCatalog.set(loader.projectConfig(), loader.graphStore());

        TestProcedureRunner.applyOnProcedure(db, LabelPropagationMutateProc.class, procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    Map<String, Object> config = Map.of(
                        "nodeLabels", Collections.singletonList("B"),
                        "mutateProperty", MUTATE_PROPERTY
                    );
                    try {
                        mutateMethod.invoke(procedure, TEST_GRAPH_NAME, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                }));

        String graphWriteQuery =
            "CALL gds.graph.nodeProperties.write(" +
            "   $graph, " +
            "   [$property]" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten";
        runQuery(graphWriteQuery, Map.of("graph", TEST_GRAPH_NAME, "property", MUTATE_PROPERTY));

        String checkNeo4jGraphNegativeQuery = formatWithLocale("MATCH (n:A) RETURN n.%s AS property", MUTATE_PROPERTY);
        QueryRunner.runQueryWithRowConsumer(
            db,
            checkNeo4jGraphNegativeQuery,
            Map.of(),
            ((transaction, resultRow) -> assertNull(resultRow.get("property")))
        );

        String checkNeo4jGraphPositiveQuery = formatWithLocale("MATCH (n:B) RETURN n.%s AS property", MUTATE_PROPERTY);
        QueryRunner.runQueryWithRowConsumer(
            db,
            checkNeo4jGraphPositiveQuery,
            Map.of(),
            ((transaction, resultRow) -> assertNotNull(resultRow.get("property")))
        );
    }

    @Test
    void testGraphMutation() {
        var graphStore = runMutation(ensureGraphExists(), Map.of("mutateProperty", MUTATE_PROPERTY));
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());
        boolean containsMutateProperty = graphStore.schema().nodeSchema()
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
        GraphStore graphStore = new TestNativeGraphLoader(db)
            .withLabels("A", "B")
            .withNodeProperties(ImmutablePropertyMappings.of())
            .withDefaultOrientation(Orientation.NATURAL)
            .graphStore();

        var graphProjectConfig = allNodesAndRelationshipsProjectConfig(TEST_GRAPH_NAME);
        GraphStoreCatalog.set(graphProjectConfig, graphStore);

        Map<String, Object> config = Map.of(
            "nodeLabels", Collections.singletonList("A"),
            "mutateProperty", MUTATE_PROPERTY
        );
        runMutation(TEST_GRAPH_NAME, config);

        var mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), TEST_GRAPH_NAME).graphStore();

        var expectedProperties = Set.of(MUTATE_PROPERTY);
        assertEquals(expectedProperties, mutatedGraph.nodePropertyKeys(NodeLabel.of("A")));
        assertEquals(new HashSet<>(), mutatedGraph.nodePropertyKeys(NodeLabel.of("B")));
    }

    @Test
    void testMutateFailsOnExistingToken() {
        String graphName = ensureGraphExists();
        TestProcedureRunner.applyOnProcedure(db, LabelPropagationMutateProc.class, procedure ->
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
                }));
        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), graphName).graphStore().getUnion();
        assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Test
    void testExceptionLogging() {
        List<TestLog> log = new ArrayList<>(1);
        assertThrows(
            NullPointerException.class,
            () -> TestProcedureRunner.applyOnProcedure(db, LabelPropagationMutateProc.class, procedure -> {
                var computationResult = mock(ComputationResult.class);
                log.add(0, ((TestLog) procedure.log));
                new LabelPropagationMutateSpecification().computationResultConsumer().consume(computationResult, procedure.executionContext());
            })
        );

        assertTrue(log.get(0).containsMessage(TestLog.WARN, "Graph mutation failed"));
    }

    @Test
    void testRunOnEmptyGraph() {
        TestProcedureRunner.applyOnProcedure(db, LabelPropagationMutateProc.class, (proc) -> {
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
                    Map<String, Object> config = Map.of("mutateProperty", MUTATE_PROPERTY);
                    try {
                        Stream<?> result = (Stream<?>) method.invoke(proc, graphName, config);
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
        GraphProjectConfig graphProjectConfig = allNodesAndRelationshipsProjectConfig(loadedGraphName);
        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());
        return loadedGraphName;
    }

    @NotNull
    private GraphStore runMutation(String graphName, Map<String, Object> config) {
        TestProcedureRunner.applyOnProcedure(db, LabelPropagationMutateProc.class, procedure ->
            ProcedureMethodHelper.mutateMethods(procedure)
                .forEach(mutateMethod -> {
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                }));

        return GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db), graphName).graphStore();
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

    private GraphProjectFromStoreConfig allNodesAndRelationshipsProjectConfig(String graphName) {
        return ImmutableGraphProjectFromStoreConfig.of(
            TEST_USERNAME,
            graphName,
            NodeProjections.create(
                Map.of(ALL_NODES, ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of()))
            ), RelationshipProjections.ALL
        );
    }

    @Nested
    class FilteredGraph extends BaseTest {

        @Neo4jGraph(offsetIds = true)
        private static final String DB_CYPHER_WITH_OFFSET = DB_CYPHER;

        @Test
        void testGraphMutationFiltered() {
            String query = GdsCypher
                .call(TEST_GRAPH_NAME)
                .algo("labelPropagation")
                .mutateMode()
                .addParameter("nodeLabels", Arrays.asList("A", "B"))
                .addParameter("mutateProperty", MUTATE_PROPERTY)
                .yields();

            runQuery(query);

            // offset is `42`
            var expectedResult = List.of(44L, 49L, 44L, 45L, 46L, 47L, 48L, 49L, 50L, 51L, 52L, 53L);

            var mutatedGraph = GraphStoreCatalog.get(
                TEST_USERNAME,
                DatabaseId.of(LabelPropagationMutateProcTest.this.db),
                TEST_GRAPH_NAME
            ).graphStore().getUnion();
            mutatedGraph.forEachNode(nodeId -> {
                    assertThat(mutatedGraph.nodeProperties("communityId").longValue(nodeId))
                        .isEqualTo(expectedResult.get(Math.toIntExact(nodeId)));
                    return true;
                }
            );
        }
    }
}
