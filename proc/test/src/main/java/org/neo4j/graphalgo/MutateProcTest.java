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
package org.neo4j.graphalgo;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.MutateProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.MutateConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

public interface MutateProcTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>, CONFIG extends MutateConfig & AlgoBaseConfig, RESULT>
    extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    default Optional<String> mutateGraphName() {
        return Optional.empty();
    }

    @Override
    default boolean supportsImplicitGraphCreate() {
        return false;
    }

    @Test
    void testGraphMutation();

    String expectedMutatedGraph();

    String failOnExistingTokenMessage();

    @Test
    default void testExceptionLogging() {
        List<TestLog> log = new ArrayList<>(1);
        assertThrows(
            NullPointerException.class,
            () -> applyOnProcedure(procedure -> {
                var computationResult = mock(AlgoBaseProc.ComputationResult.class);
                log.add(0, ((TestLog) procedure.log));
                ((MutateProc) procedure).mutate(computationResult);
            })
        );

        assertTrue(log.get(0).containsMessage(TestLog.WARN, "Graph mutation failed"));
    }

    @Test
    default void testMutateFailsOnExistingToken() {
        String graphName = ensureGraphExists();

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    CypherMapWrapper filterConfig = CypherMapWrapper.empty().withEntry(
                        "nodeLabels",
                        Collections.singletonList("A")
                    );

                    Map<String, Object> config = createMinimalConfig(filterConfig).toMap();
                    config.remove("nodeWeightProperty");
                    try {
                        // write first time
                        mutateMethod.invoke(procedure, graphName, config);
                        // write second time using same `writeProperty`
                        assertThatThrownBy(() -> mutateMethod.invoke(procedure, graphName, config))
                            .hasRootCauseInstanceOf(IllegalArgumentException.class)
                            .hasRootCauseMessage(failOnExistingTokenMessage());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore().getUnion();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Test
    default void failOnImplicitGraph() {
        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    assertThatThrownBy(() -> mutateMethod.invoke(
                        procedure,
                        Map.of("nodeProjection", "*"),
                        CypherMapWrapper.empty().toMap()
                    ))
                        .hasRootCauseInstanceOf(IllegalArgumentException.class)
                        .hasRootCauseMessage(
                            "Cannot mutate implicitly loaded graphs. Use a loaded graph in the graph-catalog"
                        );
                }));
    }

    @Override
    @Test
    default void testImplicitGraphCreateFromCypherConfig() {}

    @Override
    @Test
    default void failOnImplicitLoadingWithAlteringNodeQuery() {}

    @Override
    @Test
    default void useReadConcurrencyWhenSetOnImplicitlyLoadedGraph() {}

    @Override
    @Test
    default void testRunOnImplicitlyLoadedGraph() {}

    @Override
    @Test
    default void failOnImplicitLoadingWithAlteringRelationshipQuery() {}

    @Override
    @Test
    default void testImplicitGraphCreateFromStoreConfig() {}

    @Override
    default void failOnImplicitLoadingWithoutProjectionsOrQueries(
        String expectedMessage,
        Map<String, Object> configurationMap
    ) {}


    @NotNull
    default String ensureGraphExists() {
        return mutateGraphName().orElseGet(() -> {
            String loadedGraphName = "loadGraph";
            GraphCreateConfig graphCreateConfig = withNameAndRelationshipProjections(
                TEST_USERNAME,
                loadedGraphName,
                relationshipProjections()
            );
            GraphStoreCatalog.set(
                graphCreateConfig,
                graphLoader(graphCreateConfig).graphStore()
            );
            return loadedGraphName;
        });
    }

    @NotNull
    default GraphStore runMutation() {
        return runMutation(ensureGraphExists(), CypherMapWrapper.empty());
    }

    @NotNull
    default GraphStore runMutation(String graphName, CypherMapWrapper additionalConfig) {
        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    Map<String, Object> config = createMinimalConfig(additionalConfig).toMap();
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        return GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore();
    }
}
