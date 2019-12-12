/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
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
import static org.neo4j.graphalgo.core.utils.ExceptionUtil.rootCause;

/**
 * Base test that should be used for every algorithm procedure.
 * This test assumes that the implementing test method populates
 * the database returned by {@link AlgoBaseProcTest#graphDb} and
 * clears the data after each test.
 */
public interface AlgoBaseProcTest<CONFIG extends AlgoBaseConfig, RESULT> {

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

    default void applyOnProcedure(
        Consumer<? super AlgoBaseProc<?, RESULT, CONFIG>> func
    ) {
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
    default void testGraphCreateConfig() {
        CypherMapWrapper wrapper = createMinimalConfig(CypherMapWrapper.empty());
        applyOnProcedure(proc -> {
            CONFIG config = proc.newConfig(Optional.empty(), wrapper);
            assertEquals(Optional.empty(), config.graphName(), "Graph name should be empty.");
            Optional<GraphCreateConfig> maybeGraphCreateConfig = config.implicitCreateConfig();
            assertTrue(maybeGraphCreateConfig.isPresent(), "Config should contain a GraphCreateConfig.");
            GraphCreateConfig graphCreateConfig = maybeGraphCreateConfig.get();
            graphCreateConfig = ImmutableGraphCreateConfig.copyOf(graphCreateConfig);
            assertEquals(
                GraphCreateConfig.emptyWithName("", ""),
                graphCreateConfig,
                "GraphCreateConfig should be empty."
            );
            assertEquals(GraphCreateConfig.emptyWithName("", ""), graphCreateConfig);
        });
    }

    @Test
    default void shouldFailWithInvalidWeightProperty() {
        assumeTrue(createConfig(createMinimallyValidConfig(CypherMapWrapper.empty())) instanceof WeightConfig);
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateConfig.emptyWithName("", loadedGraphName);
        Graph graph = new GraphLoader(graphDb())
            .withGraphCreateConfig(graphCreateConfig)
            .load(HugeGraphFactory.class);

        GraphCatalog.set(graphCreateConfig, GraphsByRelationshipType.of(graph));

        applyOnProcedure((proc) -> {
            CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
                "weightProperty",
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimallyValidConfig(mapWrapper).toMap();
            String error = "Weight property `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found";
            assertMissingProperty(error, () -> proc.compute(
                loadedGraphName,
                configMap
            ));

            assertMissingProperty(error, () -> proc.compute(
                configMap,
                Collections.emptyMap()
            ));
        });

        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    default void shouldFailWithInvalidSeedProperty() {
        assumeTrue(createConfig(createMinimallyValidConfig(CypherMapWrapper.empty())) instanceof SeedConfig);
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateConfig.emptyWithName("", loadedGraphName);
        Graph graph = new GraphLoader(graphDb())
            .withGraphCreateConfig(graphCreateConfig)
            .load(HugeGraphFactory.class);

        GraphCatalog.set(graphCreateConfig, GraphsByRelationshipType.of(graph));

        applyOnProcedure((proc) -> {
            CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map(
                "seedProperty",
                "___THIS_PROPERTY_SHOULD_NOT_EXIST___"
            ));
            Map<String, Object> configMap = createMinimallyValidConfig(mapWrapper).toMap();
            String error = "Seed property `___THIS_PROPERTY_SHOULD_NOT_EXIST___` not found";
            assertMissingProperty(error, () -> proc.compute(
                loadedGraphName,
                configMap
            ));

            assertMissingProperty(error, () -> proc.compute(
                configMap,
                Collections.emptyMap()
            ));
        });

        GraphCatalog.removeAllLoadedGraphs();
    }

    default void assertMissingProperty(String error, Runnable runnable) {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            runnable::run
        );
        assertTrue(exception.getMessage().contains(error));
    }

    @Test
    default void testRunOnLoadedGraph() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateConfig.emptyWithName("", loadedGraphName);
        Graph graph = new GraphLoader(graphDb())
            .withGraphCreateConfig(graphCreateConfig)
            .load(HugeGraphFactory.class);

        GraphCatalog.set(graphCreateConfig, GraphsByRelationshipType.of(graph));

        applyOnProcedure((proc) -> {
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
    default void testRunMultipleTimesOnLoadedGraph() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateConfig.emptyWithName("", loadedGraphName);
        Graph graph = new GraphLoader(graphDb())
            .withGraphCreateConfig(graphCreateConfig)
            .load(HugeGraphFactory.class);

        GraphCatalog.set(graphCreateConfig, GraphsByRelationshipType.of(graph));

        applyOnProcedure((proc) -> {
            Map<String, Object> configMap = createMinimalConfig(CypherMapWrapper.empty()).toMap();
            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultRun1 = proc.compute(loadedGraphName, configMap);
            AlgoBaseProc.ComputationResult<?, RESULT, CONFIG> resultRun2 = proc.compute(loadedGraphName, configMap);

            assertResultEquals(resultRun1.result(), resultRun2.result());
        });
    }

    @Test
    default void testRunOnEmptyGraph() {
        graphDb().execute("MATCH (n) DETACH DELETE n");

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
    default void checkStatsModeExists() {
        applyOnProcedure((proc) -> {
            boolean inWriteClass = methodExists(proc, "write");
            if (inWriteClass) {
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
}
