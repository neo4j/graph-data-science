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
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.WriteConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.utils.ExceptionUtil;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public interface GraphMutationTest<CONFIG extends WriteConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<CONFIG, RESULT> {

    @Test
    default void testMutateNodeProperties() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateFromStoreConfig.emptyWithName(TEST_USERNAME, loadedGraphName);
        GraphStoreCatalog.set(
            graphCreateConfig,
            graphLoader(graphCreateConfig).build(NativeFactory.class).build().graphStore()
        );

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    Map<String, Object> config = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    try {
                        mutateMethod.invoke(procedure, loadedGraphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, loadedGraphName).getGraph();
        TestSupport.assertGraphEquals(TestGraph.Builder.fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    @Test
    default void testMutateFailsOnExistingNodeProperty() {
        String loadedGraphName = "loadedGraph";
        GraphCreateConfig graphCreateConfig = GraphCreateFromStoreConfig.emptyWithName(TEST_USERNAME, loadedGraphName);
        GraphStoreCatalog.set(
            graphCreateConfig,
            graphLoader(graphCreateConfig).build(NativeFactory.class).build().graphStore()
        );

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    Map<String, Object> config = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    try {
                        // write first time
                        mutateMethod.invoke(procedure, loadedGraphName, config);
                        // write second time using same `writeProperty`
                        InvocationTargetException ex = assertThrows(
                            InvocationTargetException.class,
                            () -> mutateMethod.invoke(procedure, loadedGraphName, config)
                        );

                        String expectedMessage = String.format(
                            "Node property `%s` already exists in the in-memory graph.",
                            config.get("writeProperty").toString()
                        );
                        Throwable expectedException = ExceptionUtil.rootCause(ex);
                        assertEquals(IllegalArgumentException.class, expectedException.getClass());
                        assertEquals(expectedMessage, expectedException.getMessage());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        Graph mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, loadedGraphName).getGraph();
        TestSupport.assertGraphEquals(TestGraph.Builder.fromGdl(expectedMutatedGraph()), mutatedGraph);
    }

    String expectedMutatedGraph();
}
