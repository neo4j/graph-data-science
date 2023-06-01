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
package org.neo4j.gds.modularityoptimization;

import org.assertj.core.data.Offset;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.ImmutableNodeProjection;
import org.neo4j.gds.ImmutableNodeProjections;
import org.neo4j.gds.ImmutablePropertyMappings;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.ProcedureMethodHelper;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.RelationshipProjection;
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
import org.neo4j.gds.config.GraphProjectFromStoreConfigImpl;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
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
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.GdsCypher.ExecutionModes.MUTATE;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ModularityOptimizationMutateProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String TEST_GRAPH_NAME = "myGraph";
    private static final String MUTATE_PROPERTY = "community";

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 1})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";


    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ModularityOptimizationMutateProc.class,
            GraphProjectProc.class,
            // this is needed by `MutateNodePropertyTest.testWriteBackGraphMutationOnFilteredGraph` 🤨
            GraphWriteNodePropertiesProc.class
        );

        runQuery(graphProjectQuery());
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testMutate() {
        String query = GdsCypher.call(TEST_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(0.12244, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });
    }

    @Test
    void testMutateWeighted() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getBoolean("didConverge")).isTrue();
            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .isEqualTo(0.4985, Offset.offset(0.001));
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isLessThanOrEqualTo(3);
        });
    }

    @Test
    void testMutateSeeded() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("seedProperty", "seed1")
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(query);

        GraphStore mutatedGraph = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), TEST_GRAPH_NAME).graphStore();
        var communities = mutatedGraph.nodeProperty(MUTATE_PROPERTY).values();
        var seeds = mutatedGraph.nodeProperty("seed1").values();
        for (int i = 0; i < mutatedGraph.nodeCount(); i++) {
            assertEquals(communities.longValue(i), seeds.longValue(i));
        }
    }

    @Test
    void testMutateTolerance() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("tolerance", 1)
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getBoolean("didConverge")).isTrue();

            // Cannot converge after one iteration,
            // because it doesn't have anything to compare the computed modularity against.
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(2);
        });
    }

    @Test
    void testMutateIterations() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .mutateMode()
            .addParameter("maxIterations", 1)
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getBoolean("didConverge")).isFalse();
            assertThat(row.getNumber("ranIterations"))
                .asInstanceOf(LONG)
                .isEqualTo(1);
        });
    }

    // This should not be tested here...
    @Test
    void testMutateEstimate() {
        String query = GdsCypher.call(TEST_GRAPH_NAME).algo("gds", "beta", "modularityOptimization")
            .estimationMode(MUTATE)
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertThat(row.getNumber("bytesMin"))
                .asInstanceOf(LONG)
                .isPositive();
            assertThat(row.getNumber("bytesMax"))
                .asInstanceOf(LONG)
                .isPositive();
        });
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

        TestProcedureRunner.applyOnProcedure(db, ModularityOptimizationMutateProc.class, procedure ->
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
        GraphStore graphStore = runMutation(ensureGraphExists(), Map.of("mutateProperty", MUTATE_PROPERTY));
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
        var graphStore = new TestNativeGraphLoader(db)
            .withLabels("A", "B")
            .withNodeProperties(ImmutablePropertyMappings.of())
            .withDefaultOrientation(Orientation.NATURAL)
            .graphStore();

        var graphProjectConfig = withAllNodesAndRelationshipsProjectConfig(TEST_GRAPH_NAME);
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

        // mutate first time
        // mutate second time using same `mutateProperty`
        TestProcedureRunner.applyOnProcedure(db, ModularityOptimizationMutateProc.class, procedure ->
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
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Test
    void testExceptionLogging() {
        List<TestLog> log = new ArrayList<>(1);
        assertThrows(
            NullPointerException.class,
            () -> TestProcedureRunner.applyOnProcedure(db, ModularityOptimizationMutateProc.class, procedure -> {
                var computationResult = mock(ComputationResult.class);
                log.add(0, ((TestLog) procedure.log));
                new ModularityOptimizationMutateSpecification().computationResultConsumer().consume(computationResult, procedure.executionContext());
            })
        );

        assertTrue(log.get(0).containsMessage(TestLog.WARN, "Graph mutation failed"));
    }

    @Test
    void testRunOnEmptyGraph() {
        // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
        TestProcedureRunner.applyOnProcedure(db, ModularityOptimizationMutateProc.class, (proc) -> {
            var methods = ProcedureMethodHelper.mutateMethods(proc).collect(Collectors.toList());

            if (!methods.isEmpty()) {
                // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later
                runQuery("CALL db.createLabel('X')");
                runQuery("MATCH (n) DETACH DELETE n");
                GraphStoreCatalog.removeAllLoadedGraphs();

                var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
                    TEST_USERNAME,
                    TEST_GRAPH_NAME,
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
                        Stream<?> result = (Stream<?>) method.invoke(proc, TEST_GRAPH_NAME, configMap);
                        assertEquals(1, result.count());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
            }
        });
    }

    private static String graphProjectQuery() {
        GraphProjectFromStoreConfig config = GraphProjectFromStoreConfigImpl
            .builder()
            .graphName("")
            .username("")
            .nodeProjections(NodeProjections.all())
            .nodeProperties(PropertyMappings.fromObject(Arrays.asList("seed1", "seed2")))
            .relationshipProjections(RelationshipProjections.single(
                    ALL_RELATIONSHIPS,
                    RelationshipProjection.builder()
                        .type("TYPE")
                        .orientation(Orientation.UNDIRECTED)
                        .addProperty(PropertyMapping.of("weight", 1D))
                        .build()
                )
            ).build();

        return GdsCypher
            .call(TEST_GRAPH_NAME)
            .graphProject()
            .withGraphProjectConfig(config)
            .yields();
    }

    private String expectedMutatedGraph() {
        return
            "  (a { community: 4 }) " +
            ", (b { community: 4 }) " +
            ", (c { community: 4 }) " +
            ", (d { community: 3 }) " +
            ", (e { community: 4 }) " +
            ", (f { community: 3 }) " +
            ", (a)-[]->(b)" +
            ", (a)-[]->(e)" +
            ", (a)-[]->(f)" +
            ", (b)-[]->(c)" +
            ", (b)-[]->(d)" +
            ", (c)-[]->(e)" +
            ", (f)-[]->(d)";
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
        TestProcedureRunner.applyOnProcedure(db, ModularityOptimizationMutateProc.class, procedure ->
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

    private GraphProjectFromStoreConfig withAllNodesAndRelationshipsProjectConfig(String graphName) {
        return ImmutableGraphProjectFromStoreConfig.of(
            TEST_USERNAME,
            graphName,
            NodeProjections.create(Map.of(
                ALL_NODES, ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of())
            )), RelationshipProjections.ALL
        );
    }
}
