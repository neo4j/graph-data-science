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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport.AllGraphStoreFactoryTypesTest;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.GraphCreateFromCypherConfig;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GdsEdition;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.gds.core.write.NativeNodePropertyExporter;
import org.neo4j.gds.core.write.RelationshipExporter;
import org.neo4j.gds.core.write.RelationshipStreamExporter;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.AbstractRelationshipProjection.ORIENTATION_KEY;
import static org.neo4j.gds.AbstractRelationshipProjection.TYPE_KEY;
import static org.neo4j.gds.BaseProcTest.anonymousGraphConfig;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.TestSupport.FactoryType.CYPHER;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.gds.config.GraphCreateConfig.IMPLICIT_GRAPH_NAME;
import static org.neo4j.gds.config.GraphCreateConfig.READ_CONCURRENCY_KEY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.NODE_PROPERTIES_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Base test that should be used for every algorithm procedure.
 * This test assumes that the implementing test method populates
 * the database returned by {@link AlgoBaseProcTest#graphDb} and
 * clears the data after each test.
 */
public interface AlgoBaseProcTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>, CONFIG extends AlgoBaseConfig, RESULT>
    extends GraphCreateConfigSupport
{

    String TEST_USERNAME = AuthSubject.ANONYMOUS.username();

    @AfterEach
    default void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    Class<? extends AlgoBaseProc<ALGORITHM, RESULT, CONFIG>> getProcedureClazz();

    default AlgoBaseProc<ALGORITHM, RESULT, CONFIG> proc() {
        try {
            return getProcedureClazz()
                .getConstructor()
                .newInstance();
        } catch (Exception e) {
            fail("unable to instantiate procedure", e);
        }
        return null;
    }

    default boolean supportsImplicitGraphCreate() {
        return true;
    }

    GraphDatabaseAPI graphDb();

    default NamedDatabaseId namedDatabaseId() {
        return graphDb().databaseId();
    }

    default GraphDatabaseAPI emptyDb() {
        GraphDatabaseAPI db = graphDb();
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

    default CypherMapWrapper createMinimalImplicitConfig(CypherMapWrapper mapWrapper) {
        return createMinimalConfig(CypherMapWrapper.create(anonymousGraphConfig(mapWrapper.toMap())));
    }

    default void applyOnProcedure(Consumer<? super AlgoBaseProc<ALGORITHM, RESULT, CONFIG>> func) {
        try (GraphDatabaseApiProxy.Transactions transactions = newKernelTransaction(graphDb())) {
            AlgoBaseProc<ALGORITHM, RESULT, CONFIG> proc;
            try {
                proc = getProcedureClazz().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not instantiate Procedure Class " + getProcedureClazz().getSimpleName());
            }

            proc.procedureTransaction = transactions.tx();
            proc.transaction = transactions.ktx();
            proc.api = graphDb();
            proc.callContext = ProcedureCallContext.EMPTY;
            proc.log = new TestLog();
            proc.progressTracker = EmptyProgressEventTracker.INSTANCE;

            if (proc instanceof NodePropertiesWriter) {
                ((NodePropertiesWriter<?, ?, ?>) proc).nodePropertyExporterBuilder = new NativeNodePropertyExporter.Builder(
                    TransactionContext.of(
                        proc.api,
                        proc.procedureTransaction
                    ));
            }

            if (proc instanceof WriteRelationshipsProc) {
                ((WriteRelationshipsProc<?, ?, ?, ?>) proc).relationshipExporterBuilder = new RelationshipExporter.Builder(
                    TransactionContext.of(
                        proc.api,
                        proc.procedureTransaction
                    ));
            }

            if (proc instanceof StreamOfRelationshipsWriter) {
                ((StreamOfRelationshipsWriter<?, ?, ?>) proc).relationshipStreamExporterBuilder = new RelationshipStreamExporter.Builder(
                    TransactionContext.of(
                        proc.api,
                        proc.procedureTransaction
                    ));
            }

            func.accept(proc);
        }
    }

    @Test
    default void testImplicitGraphCreateFromStoreConfig() {
        CypherMapWrapper wrapper = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_PROJECTION_KEY, Collections.singletonList("*"),
            RELATIONSHIP_PROJECTION_KEY, relationshipProjections()
        )));
        applyOnProcedure(proc -> {
            CONFIG config = proc.newConfig(Optional.empty(), wrapper);
            assertEquals(Optional.empty(), config.graphName(), "Graph name should be empty.");
            Optional<GraphCreateConfig> maybeGraphCreateConfig = config.implicitCreateConfig();
            assertTrue(maybeGraphCreateConfig.isPresent(), "Config should contain a GraphCreateConfig.");
            GraphCreateConfig actual = maybeGraphCreateConfig.get();
            assertTrue(
                actual instanceof GraphCreateFromStoreConfig,
                formatWithLocale("GraphCreateConfig should be %s.", GraphCreateFromStoreConfig.class.getSimpleName())
            );

            GraphCreateFromStoreConfig storeConfig = (GraphCreateFromStoreConfig) actual;

            NodeProjections expectedNodeProjections = expectedNodeProjections();
            RelationshipProjections expectedRelationshipProjections = relationshipProjections();

            assertEquals(expectedNodeProjections, storeConfig.nodeProjections());
            assertEquals(expectedRelationshipProjections, storeConfig.relationshipProjections());
            assertEquals(IMPLICIT_GRAPH_NAME, storeConfig.graphName());
            assertEquals(TEST_USERNAME, storeConfig.username());
        });
    }

    default NodeProjections expectedNodeProjections() {
        return NodeProjections
            .builder()
            .putProjection(ALL_NODES, NodeProjection.all())
            .build();
    }

    @Test
    default void testImplicitGraphCreateFromCypherConfig() {
        long concurrency = 2;
        Map<String, Object> tempConfig = MapUtil.map(
            NODE_QUERY_KEY, ALL_NODES_QUERY,
            RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY,
            "concurrency", concurrency
        );
        CypherMapWrapper wrapper = createMinimalConfig(CypherMapWrapper.create(tempConfig));

        applyOnProcedure(proc -> {
            CONFIG config = proc.newConfig(Optional.empty(), wrapper);
            assertEquals(Optional.empty(), config.graphName(), "Graph name should be empty.");
            Optional<GraphCreateConfig> maybeGraphCreateConfig = config.implicitCreateConfig();
            assertTrue(maybeGraphCreateConfig.isPresent(), "Config should contain a GraphCreateConfig.");
            assertTrue(
                maybeGraphCreateConfig.get() instanceof GraphCreateFromCypherConfig,
                formatWithLocale("GraphCreateConfig should be %s.", GraphCreateFromCypherConfig.class.getSimpleName()));

            GraphCreateFromCypherConfig actualConfig = (GraphCreateFromCypherConfig) maybeGraphCreateConfig.get();

            assertEquals(ALL_NODES_QUERY, actualConfig.nodeQuery());
            assertEquals(ALL_RELATIONSHIPS_QUERY, actualConfig.relationshipQuery());
            assertEquals(IMPLICIT_GRAPH_NAME, actualConfig.graphName());
            assertEquals(TEST_USERNAME, actualConfig.username());
            assertEquals(concurrency, actualConfig.readConcurrency());
        });
    }

    default void assertMissingProperty(String error, Runnable runnable) {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            runnable::run
        );
        assertThat(exception).hasMessageContaining(error);
    }

    @AllGraphStoreFactoryTypesTest
    default void testRunOnLoadedGraph(TestSupport.FactoryType factoryType) {
        // FIXME rethink this test for mutate
        if (supportsImplicitGraphCreate()) {
            String loadedGraphName = "loadedGraph";
            GraphCreateConfig graphCreateConfig = factoryType == CYPHER
                ? withNameAndRelationshipQuery("", loadedGraphName, relationshipQuery())
                : withNameAndRelationshipProjections("", loadedGraphName, relationshipProjections());

            applyOnProcedure((proc) -> {
                GraphStore graphStore = graphLoader(graphCreateConfig).graphStore();
                GraphStoreCatalog.set(
                    graphCreateConfig,
                    graphStore
                );
                Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultOnLoadedGraph = proc.compute(
                    loadedGraphName,
                    configMap
                );

                Map<String, Object> implicitConfigMap = createMinimalImplicitConfig(CypherMapWrapper.empty()).toMap();
                AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultOnImplicitGraph = proc.compute(
                    implicitConfigMap,
                    Collections.emptyMap()
                );

                assertResultEquals(resultOnImplicitGraph.result(), resultOnLoadedGraph.result());
            });
        }
    }

    @Test
    default void testRunOnImplicitlyLoadedGraph() {
        Map<String, Object> cypherConfig = createMinimalImplicitConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_QUERY_KEY, ALL_NODES_QUERY,
            RELATIONSHIP_QUERY_KEY, relationshipQuery()
        ))).toMap();

        Map<String, Object> storeConfig = createMinimalImplicitConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_PROJECTION_KEY, Collections.singletonList("*"),
            RELATIONSHIP_PROJECTION_KEY, relationshipProjections()
        ))).toMap();

        applyOnProcedure((proc) -> {

            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultOnImplicitGraphFromCypher = proc.compute(
                cypherConfig,
                Collections.emptyMap()
            );

            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultOnImplicitGraphFromStore = proc.compute(
                storeConfig,
                Collections.emptyMap()
            );

            assertResultEquals(resultOnImplicitGraphFromCypher.result(), resultOnImplicitGraphFromStore.result());
        });
    }

    @Test
    default void useReadConcurrencyWhenSetOnImplicitlyLoadedGraph() {
        CypherMapWrapper config = createMinimalConfig(
            CypherMapWrapper.create(
                Map.of(
                    NODE_PROJECTION_KEY, NodeProjections.ALL,
                    RELATIONSHIP_PROJECTION_KEY, relationshipProjections(),
                    READ_CONCURRENCY_KEY, 2
                )
            )
        );

        applyOnProcedure((proc) -> {
            CONFIG algoConfig = proc.newConfig(Optional.empty(), config);
            assertTrue(algoConfig.implicitCreateConfig().isPresent());
            assertEquals(2, algoConfig.implicitCreateConfig().get().readConcurrency());
        });
    }

    default RelationshipProjections relationshipProjections() {
        return RelationshipProjections.ALL;
    }

    default String relationshipQuery() {
        return ALL_RELATIONSHIPS_QUERY;
    }

    @AllGraphStoreFactoryTypesTest
    default void testRunMultipleTimesOnLoadedGraph(TestSupport.FactoryType factoryType) {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = factoryType == CYPHER
            ? emptyWithNameCypher(TEST_USERNAME, loadedGraphName)
            : withNameAndRelationshipProjections(TEST_USERNAME, loadedGraphName, relationshipProjections());

        applyOnProcedure((proc) -> {
            GraphStoreCatalog.set(
                graphCreateConfig,
                graphLoader(graphCreateConfig).graphStore()
            );
            Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultRun1 = proc.compute(loadedGraphName, configMap);
            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultRun2 = proc.compute(loadedGraphName, configMap);

            assertResultEquals(resultRun1.result(), resultRun2.result());
        });
    }

    @Test
    default void testRunOnEmptyGraph() {
        runQuery(graphDb(), "MATCH (n) DETACH DELETE n");

        applyOnProcedure((proc) -> {
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    Map<String, Object> configMap = createMinimalImplicitConfig(CypherMapWrapper.empty()).toMap();
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
                        Stream<?> result = (Stream<?>) method.invoke(proc, configMap, Collections.emptyMap());

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

    default void loadGraph(String graphName){
        runQuery(
            graphDb(),
            GdsCypher.call()
                .loadEverything()
                .graphCreate(graphName)
                .yields()
        );
    }

    @Test
    default void testFailOnMissingNodeLabel() {
        applyOnProcedure((proc) -> {
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    String missingLabel = "___THIS_LABEL_SHOULD_NOT_EXIST___";
                    Map<String, Object> tempConfig = MapUtil.map(NODE_PROJECTION_KEY, Collections.singletonList(missingLabel));

                    Map<String, Object> configMap = createMinimalImplicitConfig(CypherMapWrapper.create(tempConfig)).toMap();

                    Exception ex = assertThrows(
                        Exception.class,
                        () -> method.invoke(proc, configMap, Collections.emptyMap())
                    );
                    assertThat(ex)
                        .getRootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(formatWithLocale(
                            "Invalid node projection, one or more labels not found: '%s'",
                            missingLabel
                    ));
                });
        });
    }

    @Test
    default void shouldThrowWhenTooManyCoresOnLimited() {
        GdsEdition.instance().setToCommunityEdition();
        applyOnProcedure((proc) ->
            getWriteAndStreamProcedures(proc).forEach(method -> {
                Map<String, Object> configMap = createMinimalImplicitConfig(CypherMapWrapper.create(MapUtil.map(
                    "concurrency",
                    10
                ))).toMap();

                InvocationTargetException ex = assertThrows(
                    InvocationTargetException.class,
                    () -> method.invoke(proc, configMap, Collections.emptyMap())
                );
                assertThat(ex)
                    .getRootCause()
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Community users cannot exceed readConcurrency=4 (you configured readConcurrency=10), see https://neo4j.com/docs/graph-data-science/");
            })
        );
    }

    @Test
    @GdsEditionTest(Edition.EE)
    default void shouldAllowManyCoresOnUnlimited() {
        applyOnProcedure((proc) ->
            getWriteAndStreamProcedures(proc).forEach(method -> {
                Map<String, Object> configMap = createMinimalImplicitConfig(CypherMapWrapper.create(MapUtil.map("concurrency", 78))).toMap();

                assertDoesNotThrow(() -> method.invoke(proc, configMap, Collections.emptyMap()));
            })
        );
    }

    @Test
    default void testFailOnMissingRelationshipType() {
        applyOnProcedure((proc) -> {
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    String missingRelType = "___THIS_REL_TYPE_SHOULD_NOT_EXIST___";
                    Map<String, Object> tempConfig = Map.of(RELATIONSHIP_PROJECTION_KEY, relationshipProjectionForType(missingRelType));

                    Map<String, Object> configMap = createMinimalImplicitConfig(CypherMapWrapper.create(tempConfig)).toMap();

                    Exception ex = assertThrows(
                        Exception.class,
                        () -> method.invoke(proc, configMap, Collections.emptyMap())
                    );
                    assertThat(ex)
                        .getRootCause()
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(formatWithLocale(
                            "Invalid relationship projection, one or more relationship types not found: '%s'",
                            missingRelType
                        ));
                });
        });
    }

    private Object relationshipProjectionForType(String type) {
        return this instanceof OnlyUndirectedTest<?, ?, ?>
            ? Map.of(type, Map.of(TYPE_KEY, type, ORIENTATION_KEY, Orientation.UNDIRECTED.name()))
            : List.of(type);
    }

    @Test
    default void failOnImplicitLoadingWithAlteringNodeQuery() {
        Map<String, Object> config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_QUERY_KEY, "MATCH (n) SET n.name='foo' RETURN id(n) AS id",
            RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY
        ))).toMap();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> applyOnProcedure((proc) -> proc.compute(
                config,
                Collections.emptyMap()
            ))
        );

        assertThat(ex)
            .hasMessageContaining("Query must be read only. Query: ");
    }

    // NOTE: this test needs at least one relationship in order to pass
    @Test
    default void failOnImplicitLoadingWithAlteringRelationshipQuery() {
        Map<String, Object> config = createMinimalImplicitConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_QUERY_KEY, ALL_NODES_QUERY,
            RELATIONSHIP_QUERY_KEY, "MATCH (s)-->(t) SET s.foo=false RETURN id(s) AS source, id(t) as target"
        ))).toMap();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> applyOnProcedure((proc) -> proc.compute(
                config,
                Collections.emptyMap()
            ))
        );
        assertThat(ex)
            .hasMessageContaining("Query must be read only. Query: ");
    }

    String FAIL_ANY_CONFIG = formatWithLocale(
        "`%s` and `%s` or `%s` and `%s`",
        NODE_PROJECTION_KEY,
        RELATIONSHIP_PROJECTION_KEY,
        NODE_QUERY_KEY,
        RELATIONSHIP_QUERY_KEY
    );
    // No value specified for the mandatory configuration parameter `relationshipProjection`
    static Stream<Arguments> failingConfigurationMaps() {
        return Stream.of(
            Arguments.of(FAIL_ANY_CONFIG, MapUtil.map()),
            Arguments.of("No value specified for the mandatory configuration parameter `relationshipProjection`", MapUtil.map(NODE_PROJECTION_KEY, ALL_NODES.name)),
            Arguments.of("No value specified for the mandatory configuration parameter `nodeProjection`", MapUtil.map(RELATIONSHIP_PROJECTION_KEY, ALL_RELATIONSHIPS.name)),
            Arguments.of("No value specified for the mandatory configuration parameter `relationshipQuery`", MapUtil.map(NODE_QUERY_KEY, ALL_NODES_QUERY)),
            Arguments.of("No value specified for the mandatory configuration parameter `nodeQuery`", MapUtil.map(RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY)),
            Arguments.of(FAIL_ANY_CONFIG, MapUtil.map(NODE_PROJECTION_KEY, ALL_NODES.name, RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY)),
            Arguments.of(FAIL_ANY_CONFIG, MapUtil.map(RELATIONSHIP_PROJECTION_KEY, ALL_RELATIONSHIPS.name, NODE_QUERY_KEY, ALL_NODES_QUERY))
        );
    }

    @ParameterizedTest
    @MethodSource("failingConfigurationMaps")
    default void failOnImplicitLoadingWithoutProjectionsOrQueries(String expectedMessage, Map<String, Object> configurationMap) {
        Map<String, Object> config = createMinimalConfig(CypherMapWrapper.create(configurationMap)).toMap();
        config.remove("nodeWeightProperty");

        applyOnProcedure((proc) -> {
            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> proc.compute(
                    config,
                    Collections.emptyMap()
                )
            );
            String message = ex.getMessage();
            assertTrue(
                message.contains("Missing information"),
                formatWithLocale("Does not start with 'Missing information': %s", message)
            );
            assertTrue(
                message.contains(expectedMessage),
                formatWithLocale("Does not contain '%s': %s", expectedMessage, message)
            );
        });
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

    default boolean methodExists(AlgoBaseProc<?, RESULT, CONFIG> proc, String methodSuffix) {
        return getProcedureMethods(proc)
            .anyMatch(method -> getProcedureMethodName(method).endsWith(methodSuffix));
    }

    default Stream<Method> getProcedureMethods(AlgoBaseProc<?, RESULT, CONFIG> proc) {
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

    default Stream<Method> getWriteAndStreamProcedures(AlgoBaseProc<?, RESULT, CONFIG> proc) {
        return getProcedureMethods(proc)
            .filter(method -> {
                String procedureMethodName = getProcedureMethodName(method);
                return procedureMethodName.endsWith("stream") || procedureMethodName.endsWith("write");
            });
    }

    default Stream<Method> getWriteStreamStatsProcedures(AlgoBaseProc<?, RESULT, CONFIG> proc) {
        return getProcedureMethods(proc)
            .filter(method -> {
                var procedureMethodName = getProcedureMethodName(method);
                return procedureMethodName.endsWith("stream") || procedureMethodName.endsWith("write") || procedureMethodName.endsWith("stats");
            });
    }

    @NotNull
    default GraphLoader graphLoader(GraphCreateConfig graphCreateConfig) {
        return graphLoader(graphDb(), graphCreateConfig);
    }

    @NotNull
    default GraphLoader graphLoader(
        GraphDatabaseAPI db,
        GraphCreateConfig graphCreateConfig
    ) {
        return ImmutableGraphLoader
            .builder()
            .context(ImmutableGraphLoaderContext.builder()
                .transactionContext(TestSupport.fullAccessTransaction(db))
                .api(db)
                .log(new TestLog())
                .build())
            .username("")
            .createConfig(graphCreateConfig)
            .build();
    }

}
