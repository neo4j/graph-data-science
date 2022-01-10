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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface MemoryEstimateTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    @Test
    default void testEstimateExistsForEveryProcedure() {
        applyOnProcedure(proc -> {
            getProcedureMethods(proc)
                .filter(procMethod -> !getProcedureMethodName(procMethod).endsWith(".estimate"))
                .forEach(noneEstimateMethod -> {
                    String procName = getProcedureMethodName(noneEstimateMethod);
                    boolean estimateProcExists = getProcedureMethods(proc)
                        .map(this::getProcedureMethodName)
                        .anyMatch(otherProcName -> otherProcName.equals(procName + ".estimate"));
                    assertTrue(estimateProcExists, formatWithLocale("Could not find estimate method for %s", procName));
                });
        });
    }

    @Test
    default void testMemoryEstimate() {
        applyOnProcedure(proc -> {
            getProcedureMethods(proc)
                .filter(procMethod -> getProcedureMethodName(procMethod).endsWith(".estimate"))
                .forEach(estimateMethod -> {
                    Map<String, Object> config = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    try {
                        var graphName = "memoryEstimateTestGraph";
                        loadGraph(graphName);
                        Stream<MemoryEstimateResult> result = (Stream) estimateMethod.invoke(proc, graphName, config);
                        result.forEach(row -> {
                            assertTrue(row.nodeCount > 0);
                            assertTrue(row.bytesMin > 0);
                            assertTrue(row.bytesMax >= row.bytesMin);
                            assertNotNull(row.mapView);
                            assertFalse(row.treeView.isEmpty());
                        });
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
        });
    }

    @Test
    default void testMemoryEstimateOnExplicitDimensions() {
        boolean isAllUndirected = relationshipProjections().equals(AbstractRelationshipProjections.ALL_UNDIRECTED);

        applyOnProcedure(proc -> {
            getProcedureMethods(proc)
                .filter(procMethod -> getProcedureMethodName(procMethod).endsWith(".estimate"))
                .forEach(estimateMethod -> {
                    Map<String, Object> algoConfig = createMinimalConfig(CypherMapWrapper.empty()).toMap();
                    Map<String, Object> graphProjectConfig = CypherMapWrapper.empty()
                        .withEntry(GraphProjectFromStoreConfig.NODE_PROJECTION_KEY, NodeProjections.all())
                        .withEntry(GraphProjectFromStoreConfig.RELATIONSHIP_PROJECTION_KEY, relationshipProjections())
                        .withNumber("nodeCount", 100_000_000L)
                        .withNumber("relationshipCount", 20_000_000_000L)
                        .withoutEntry("nodeProperties")
                        .toMap();
                    try {
                        Stream<MemoryEstimateResult> result = (Stream) estimateMethod.invoke(
                            proc,
                            graphProjectConfig,
                            algoConfig
                        );
                        result.forEach(row -> {
                            assertEquals(100_000_000L, row.nodeCount);
                            assertEquals(20_000_000_000L, row.relationshipCount);
                            var components = (List<Map<String, Object>>) row.mapView.get("components");
                            assertEquals(2, components.size());

                            var graphComponent = components.get(0);
                            assertEquals("graph", graphComponent.get("name"));
                            if (isAllUndirected) {
                                assertEquals("[39 GiB ... 114 GiB]", graphComponent.get("memoryUsage"));
                            } else {
                                assertEquals("[21 GiB ... 58 GiB]", graphComponent.get("memoryUsage"));
                            }

                            assertTrue(row.bytesMin > 0);
                            assertTrue(row.bytesMax >= row.bytesMin);
                            assertNotNull(row.mapView);
                            assertFalse(row.treeView.isEmpty());
                        });
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                });
        });
    }
}
