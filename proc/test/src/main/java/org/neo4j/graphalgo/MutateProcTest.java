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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.MutateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.utils.ExceptionUtil;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestSupport.FactoryType.NATIVE;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface MutateProcTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>, CONFIG extends MutateConfig & AlgoBaseConfig, RESULT>
    extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    default Optional<String> mutateGraphName() {
        return Optional.empty();
    }

    String mutateProperty();

    ValueType mutatePropertyType();

    @Override
    default CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            mapWrapper = mapWrapper.withString("mutateProperty", mutateProperty());
        }
        return mapWrapper;
    }

    @Test
    default void testGraphMutation() {
        String graphName = mutateGraphName().orElseGet(() -> {
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

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    Map<String, Object> config = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        GraphStore graphStore = GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());

        GraphSchema schema = graphStore.schema();
        boolean nodesContainMutateProperty = containsMutateProperty(schema.nodeSchema().properties());
        boolean relationshipsContainMutateProperty = containsMutateProperty(schema.relationshipSchema().properties());
        assertTrue(nodesContainMutateProperty || relationshipsContainMutateProperty);
    }

    default boolean containsMutateProperty(Map<?, Map<String, ValueType>> entitySchema) {
        return entitySchema
            .values()
            .stream()
            .anyMatch(props -> props.containsKey(mutateProperty()) && props.get(mutateProperty()) == mutatePropertyType());
    }

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
    default void testGraphMutationOnFilteredGraph() {
        runQuery(graphDb(), "MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery(graphDb(), "CREATE (a1: A), (a2: A), (b: B), (a1)-[:REL]->(a2)");
        GraphStore graphStore = TestGraphLoader
            .from(graphDb())
            .withLabels("A", "B")
            .withRelationshipTypes("REL")
            .graphStore(NATIVE);

        String graphName = "myGraph";
        var createConfig = withNameAndRelationshipProjections("", graphName, relationshipProjections());
        GraphStoreCatalog.set(createConfig, graphStore);

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    CypherMapWrapper filterConfig = CypherMapWrapper.empty().withEntry(
                        "nodeLabels",
                        Collections.singletonList("A")
                    );

                    Map<String, Object> config = createMinimalConfig(filterConfig).toMap();
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        GraphStore mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore();
        assertEquals(
            Collections.singleton(mutateProperty()),
            mutatedGraph.nodePropertyKeys(NodeLabel.of("A"))
        );

        assertEquals(
            Collections.emptySet(),
            mutatedGraph.nodePropertyKeys(NodeLabel.of("B"))
        );
    }

    @Test
    default void testMutateFailsOnExistingToken() {
        String graphName = mutateGraphName().orElseGet(() -> {
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

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    Map<String, Object> config = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    try {
                        // write first time
                        mutateMethod.invoke(procedure, graphName, config);
                        // write second time using same `writeProperty`
                        InvocationTargetException ex = assertThrows(
                            InvocationTargetException.class,
                            () -> mutateMethod.invoke(procedure, graphName, config)
                        );

                        Throwable expectedException = ExceptionUtil.rootCause(ex);
                        assertEquals(IllegalArgumentException.class, expectedException.getClass());
                        assertEquals(failOnExistingTokenMessage(), expectedException.getMessage());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, namedDatabaseId(), graphName).graphStore().getUnion();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Test
    void testWriteBackGraphMutationOnFilteredGraph();

    String expectedMutatedGraph();

    default String failOnExistingTokenMessage() {
        return formatWithLocale(
            "Node property `%s` already exists in the in-memory graph.",
            mutateProperty()
        );
    }
}
