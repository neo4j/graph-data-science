/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.ImmutableModernGraphLoader;
import org.neo4j.graphalgo.core.ModernGraphLoader;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.core.utils.ExceptionUtil.rootCause;
import static org.neo4j.graphalgo.newapi.GraphCreateConfig.IMPLICIT_GRAPH_NAME;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

/**
 * Base test that should be used for every algorithm procedure.
 * This test assumes that the implementing test method populates
 * the database returned by {@link AlgoBaseProcTest#graphDb} and
 * clears the data after each test.
 */
public interface AlgoBaseProcTest<CONFIG extends AlgoBaseConfig, RESULT> {

    String TEST_USERNAME = AuthSubject.ANONYMOUS.username();

    static Stream<String> emptyStringPropertyValues() {
        return Stream.of(null, "");
    }

    @AfterEach
    default void removeAllLoadedGraphs() {
        GraphCatalog.removeAllLoadedGraphs();
    }

    Class<? extends AlgoBaseProc<?, RESULT, CONFIG>> getProcedureClazz();

    GraphDatabaseAPI graphDb();

    CONFIG createConfig(CypherMapWrapper mapWrapper);

    void assertResultEquals(RESULT result1, RESULT result2);

    default CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper;
    }

    default void applyOnProcedure(Consumer<? super AlgoBaseProc<?, RESULT, CONFIG>> func) {
        new TransactionWrapper(graphDb()).accept((tx -> {
            AlgoBaseProc<?, RESULT, CONFIG> proc;
            try {
                proc = getProcedureClazz().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not instantiate Procedure Class " + getProcedureClazz().getSimpleName());
            }

            proc.transaction = tx;
            proc.api = graphDb();
            proc.callContext = ProcedureCallContext.EMPTY;
            proc.log = new TestLog();

            func.accept(proc);
        }));
    }

    @Test
    default void testImplicitGraphCreateFromStoreConfig() {
        CypherMapWrapper wrapper = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_PROJECTION_KEY, Collections.singletonList("*"),
            RELATIONSHIP_PROJECTION_KEY, Collections.singletonList("*")
        )));
        applyOnProcedure(proc -> {
            CONFIG config = proc.newConfig(Optional.empty(), wrapper);
            assertEquals(Optional.empty(), config.graphName(), "Graph name should be empty.");
            Optional<GraphCreateConfig> maybeGraphCreateConfig = config.implicitCreateConfig();
            assertTrue(maybeGraphCreateConfig.isPresent(), "Config should contain a GraphCreateConfig.");
            GraphCreateConfig actual = maybeGraphCreateConfig.get();
            assertTrue(
                actual instanceof GraphCreateFromStoreConfig,
                String.format("GraphCreateConfig should be %s.", GraphCreateFromStoreConfig.class.getSimpleName()));

            NodeProjections expectedNodeProjections = NodeProjections
                .builder()
                .putProjection(PROJECT_ALL, NodeProjection.empty())
                .build();
            RelationshipProjections expectedRelationshipProjections = RelationshipProjections
                .builder()
                .putProjection(PROJECT_ALL, RelationshipProjection.empty())
                .build();

            assertEquals(expectedNodeProjections, actual.nodeProjection());
            assertEquals(expectedRelationshipProjections, actual.relationshipProjection());
            assertEquals(IMPLICIT_GRAPH_NAME, actual.graphName());
            assertEquals(TEST_USERNAME, actual.username());;
        });
    }

    @Test
    default void testImplicitGraphCreateFromCypherConfig() {
        Map<String, Object> tempConfig = MapUtil.map(
            NODE_QUERY_KEY, ALL_NODES_QUERY,
            RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY
        );
        CypherMapWrapper wrapper = createMinimalConfig(CypherMapWrapper.create(tempConfig));

        applyOnProcedure(proc -> {
            CONFIG config = proc.newConfig(Optional.empty(), wrapper);
            assertEquals(Optional.empty(), config.graphName(), "Graph name should be empty.");
            Optional<GraphCreateConfig> maybeGraphCreateConfig = config.implicitCreateConfig();
            assertTrue(maybeGraphCreateConfig.isPresent(), "Config should contain a GraphCreateConfig.");
            assertTrue(
                maybeGraphCreateConfig.get() instanceof GraphCreateFromCypherConfig,
                String.format("GraphCreateConfig should be %s.", GraphCreateFromCypherConfig.class.getSimpleName()));

            GraphCreateFromCypherConfig actualConfig = (GraphCreateFromCypherConfig) maybeGraphCreateConfig.get();

            assertEquals(NodeProjections.of(), actualConfig.nodeProjection());
            assertEquals(RelationshipProjections.of(), actualConfig.relationshipProjection());
            assertEquals(ALL_NODES_QUERY, actualConfig.nodeQuery());
            assertEquals(ALL_RELATIONSHIPS_QUERY, actualConfig.relationshipQuery());
            assertEquals(IMPLICIT_GRAPH_NAME, actualConfig.graphName());
            assertEquals(TEST_USERNAME, actualConfig.username());
        });
    }

    default void assertMissingProperty(String error, Runnable runnable) {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            runnable::run
        );
        assertTrue(exception.getMessage().contains(error));
    }

    @AllGraphTypesTest
    default void testRunOnLoadedGraph(Class<? extends GraphFactory> graphFactory) {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = (graphFactory.isAssignableFrom(HugeGraphFactory.class))
            ? GraphCreateFromStoreConfig.emptyWithName("", loadedGraphName)
            : GraphCreateFromCypherConfig.emptyWithName("", loadedGraphName);

        applyOnProcedure((proc) -> {
            GraphCatalog.set(
                graphCreateConfig,
                graphLoader(graphCreateConfig).build(graphFactory).build().graphs()
            );

            Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultOnLoadedGraph = proc.compute(
                loadedGraphName,
                configMap
            );

            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultOnImplicitGraph = proc.compute(
                configMap,
                Collections.emptyMap()
            );

            assertResultEquals(resultOnImplicitGraph.result(), resultOnLoadedGraph.result());
        });
    }

    @Test
    default void testRunOnImplicitlyLoadedGraph() {
        Map<String, Object> cypherConfig = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_QUERY_KEY, ALL_NODES_QUERY,
            RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY
        ))).toMap();

        Map<String, Object> storeConfig = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
            NODE_PROJECTION_KEY, Collections.singletonList("*"),
            RELATIONSHIP_PROJECTION_KEY, Collections.singletonList("*")
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

    @AllGraphTypesTest
    default void testRunMultipleTimesOnLoadedGraph(Class<? extends GraphFactory> graphFactory) {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = (graphFactory.isAssignableFrom(HugeGraphFactory.class))
            ? GraphCreateFromStoreConfig.emptyWithName(TEST_USERNAME, loadedGraphName)
            : GraphCreateFromCypherConfig.emptyWithName(TEST_USERNAME, loadedGraphName);

        applyOnProcedure((proc) -> {
            GraphCatalog.set(graphCreateConfig, graphLoader(graphCreateConfig).build(graphFactory).build().graphs());

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
                    Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();

                    try {
                        Stream<?> result = (Stream) method.invoke(proc, configMap, Collections.emptyMap());

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

    @Test
    default void testFailOnMissingNodeLabel() {
        applyOnProcedure((proc) -> {
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    String missingLabel = "___THIS_LABEL_SHOULD_NOT_EXIST___";
                    Map<String, Object> tempConfig = MapUtil.map(
                        "nodeProjection",
                        Collections.singletonList(missingLabel)
                    );

                    Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.create(tempConfig)).toMap();

                    Exception ex = assertThrows(
                        Exception.class,
                        () -> method.invoke(proc, configMap, Collections.emptyMap())
                    );
                    Throwable rootCause = rootCause(ex);
                    assertEquals(IllegalArgumentException.class, rootCause.getClass());
                    assertThat(
                        rootCause.getMessage(),
                        containsString(String.format(
                            "Invalid node projection, one or more labels not found: '%s'",
                            missingLabel
                        ))
                    );
                });
        });
    }

    @Test
    default void testFailOnMissingRelationshipType() {
        applyOnProcedure((proc) -> {
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    String missingRelType = "___THIS_REL_TYPE_SHOULD_NOT_EXIST___";
                    Map<String, Object> tempConfig = MapUtil.map(
                        "relationshipProjection",
                        Collections.singletonList(missingRelType)
                    );

                    Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.create(tempConfig)).toMap();

                    Exception ex = assertThrows(
                        Exception.class,
                        () -> method.invoke(proc, configMap, Collections.emptyMap())
                    );
                    Throwable rootCause = rootCause(ex);
                    assertEquals(IllegalArgumentException.class, rootCause.getClass());
                    assertThat(
                        rootCause.getMessage(),
                        containsString(String.format(
                            "Invalid relationship projection, one or more relationship types not found: '%s'",
                            missingRelType
                        ))
                    );
                });
        });
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

        assertThat(ex.getMessage(), containsString("Query must be read only. Query: "));
    }

    @Test
    default void failOnImplicitLoadingWithAlteringRelationshipQuery() {
        Map<String, Object> config = createMinimalConfig(CypherMapWrapper.create(MapUtil.map(
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

        assertThat(ex.getMessage(), containsString("Query must be read only. Query: "));
    }

    @Test
    default void checkStatsModeExists() {
        applyOnProcedure((proc) -> {
            boolean inStreamClass = methodExists(proc, "stream");
            if (inStreamClass) {
                assertTrue(
                    methodExists(proc, "stats"),
                    String.format("Expected %s to have a `stats` method", proc.getClass().getSimpleName())
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

    @NotNull
    default ModernGraphLoader graphLoader(GraphCreateConfig graphCreateConfig) {
        return graphLoader(graphDb(), graphCreateConfig);
    }

    @NotNull
    default ModernGraphLoader graphLoader(GraphDatabaseAPI db, GraphCreateConfig graphCreateConfig) {
        return ImmutableModernGraphLoader
            .builder()
            .api(db)
            .username("")
            .log(new TestLog())
            .createConfig(graphCreateConfig).build();
    }

}
