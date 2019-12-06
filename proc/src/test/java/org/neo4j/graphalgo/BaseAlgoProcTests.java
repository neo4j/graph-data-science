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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.newapi.BaseAlgoConfig;
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
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Suite of Base test that should be used for every algorithm procedure.
 * This test assumes that the implementing test method populates the database returned by `graphDb` and clears the
 * data after each test.
 */
public interface BaseAlgoProcTests<CONFIG extends BaseAlgoConfig, RESULT> {

    static Stream<String> emptyStringPropertyValues() {
        return Stream.of(null, "");
    }

    Class<? extends BaseAlgoProc<?, RESULT, CONFIG>> getProcedureClazz();

    GraphDatabaseAPI graphDb();

    CONFIG createConfig(CypherMapWrapper mapWrapper);

    void compareResults(RESULT result1, RESULT result2);

    default CypherMapWrapper createMinimallyValidConfig(CypherMapWrapper mapWrapper) {
        return mapWrapper;
    }

    default void applyOnProcedure(
        Consumer<? super BaseAlgoProc<?, RESULT, CONFIG>> func
    ) {
        new TransactionWrapper(graphDb()).accept((tx -> {
            BaseAlgoProc<?, RESULT, CONFIG> proc;
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
    default void testImplicitGraphLoading() {
        CypherMapWrapper wrapper = createMinimallyValidConfig(CypherMapWrapper.empty());
        applyOnProcedure(proc -> {
            CONFIG config = proc.newConfig(Optional.empty(), wrapper);
            assertEquals(Optional.empty(), config.graphName());
            Optional<GraphCreateConfig> maybeGraphCreateConfig = config.implicitCreateConfig();
            assertTrue(maybeGraphCreateConfig.isPresent());
            GraphCreateConfig graphCreateConfig = maybeGraphCreateConfig.get();
            graphCreateConfig = ImmutableGraphCreateConfig.copyOf(graphCreateConfig);
            assertEquals(GraphCreateConfig.emptyWithName("", ""), graphCreateConfig);
        });
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
            Map<String, Object> configMap = createMinimallyValidConfig(CypherMapWrapper.empty()).toMap();
            BaseAlgoProc.ComputationResult<?, RESULT, CONFIG> resultOnLoadedGraph = proc.compute(
                loadedGraphName,
                configMap
            );

            BaseAlgoProc.ComputationResult<?, RESULT, CONFIG> resultOnImplicitGraph = proc.compute(
                configMap,
                Collections.emptyMap()
            );

            compareResults(resultOnImplicitGraph.result(), resultOnLoadedGraph.result());
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
            Map<String, Object> configMap = createMinimallyValidConfig(CypherMapWrapper.empty()).toMap();
            BaseAlgoProc.ComputationResult<?, RESULT, CONFIG> resultRun1 = proc.compute(loadedGraphName, configMap);
            BaseAlgoProc.ComputationResult<?, RESULT, CONFIG> resultRun2 = proc.compute(loadedGraphName, configMap);

            compareResults(resultRun1.result(), resultRun2.result());
        });
    }

    @Test
    default void testRunOnEmptyGraph() {
        graphDb().execute("MATCH (n) DETACH DELETE n");

        applyOnProcedure((proc) -> {
            getWriteAndStreamProcedures(proc)
                .forEach(method -> {
                    Map<String, Object> configMap = createMinimallyValidConfig(CypherMapWrapper.empty()).toMap();

                    try {
                        Stream<?> result = (Stream)method.invoke(proc, configMap, Collections.emptyMap());

                        if(getProcedureMethodName(method).endsWith("stream")) {
                            assertEquals(0, result.count());
                        } else {
                            assertEquals(1, result.count());
                        }

                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
        });
    }

    default Stream<Method> getProcedureMethods(BaseAlgoProc<?, RESULT, CONFIG> proc) {
        return Arrays.stream(proc.getClass().getDeclaredMethods())
            .filter(method -> method.getDeclaredAnnotation(Procedure.class) != null);
    }

    default String getProcedureMethodName(Method method) {
        return method.getDeclaredAnnotation(Procedure.class).value();
    }

    default Stream<Method> getWriteAndStreamProcedures(BaseAlgoProc<?, RESULT, CONFIG> proc) {
        return getProcedureMethods(proc)
            .filter(method -> {
                String procedureMethodName = getProcedureMethodName(method);
                return procedureMethodName.endsWith("stream") || procedureMethodName.endsWith("write");
            });
    }
}
