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

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.wcc.WccStreamProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WccProcTest2 extends ProcTestBase {

    @BeforeEach
    void setup() throws Exception {
        String createGraph =
                "CREATE" +
                " (nA:Label {nodeId: 0, seed: 42})" +
                ",(nB:Label {nodeId: 1, seed: 42})" +
                ",(nC:Label {nodeId: 2, seed: 42})" +
                ",(nD:Label {nodeId: 3, seed: 42})" +
                ",(nE {nodeId: 4})" +
                ",(nF {nodeId: 5})" +
                ",(nG {nodeId: 6})" +
                ",(nH {nodeId: 7})" +
                ",(nI {nodeId: 8})" +
                // {J}
                ",(nJ {nodeId: 9})" +
                // {A, B, C, D}
                ",(nA)-[:TYPE]->(nB)" +
                ",(nB)-[:TYPE]->(nC)" +
                ",(nC)-[:TYPE]->(nD)" +
                ",(nD)-[:TYPE {cost:4.2}]->(nE)" + // threshold UF should split here
                // {E, F, G}
                ",(nE)-[:TYPE]->(nF)" +
                ",(nF)-[:TYPE]->(nG)" +
                // {H, I}
                ",(nH)-[:TYPE]->(nI)" +
                // {H, I, J} if TYPE_1 is considered
                ",(nI)-[:TYPE_1]->(nJ)";

        db = TestDatabaseCreator.createTestDatabase();
        runQuery(createGraph);
        registerProcedures(WccStreamProc.class, WccWriteProc.class, GraphLoadProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphNamesTest
    void testUnionFind(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                }
        );

        runQuery(
                "MATCH (n) RETURN n.partition AS partition",
                row -> assertTrue(row.getNumber("partition").longValue() < 42)
        );

        runQuery(
                "MATCH (n) RETURN n.nodeId AS nodeId, n.partition AS partition",
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    final long partitionId = row.getNumber("partition").longValue();
                    if (nodeId >= 0 && nodeId <= 6) {
                        assertTrue(partitionId >= 0 && partitionId <= 6);
                    } else {
                        assertTrue(partitionId > 6 && partitionId < 42);
                    }
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindWithLabel(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    'Label', 'TYPE', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertEquals(1L, row.getNumber("communityCount"));
                    assertEquals(1L, row.getNumber("setCount"));
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindWithSeed(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph, seedProperty: 'seed'" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                }
        );

        runQuery(
                "MATCH (n) RETURN n.partition AS partition",
                row -> assertTrue(row.getNumber("partition").longValue() >= 42)
        );

        runQuery(
                "MATCH (n) RETURN n.nodeId AS nodeId, n.partition AS partition",
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    final long partitionId = row.getNumber("partition").longValue();
                    if (nodeId >= 0 && nodeId <= 6) {
                        assertEquals(42, partitionId);
                    } else {
                        assertTrue(partitionId != 42);
                    }
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindThrowsExceptionWhenInitialSeedDoesNotExists(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph, seedProperty: 'does_not_exist'" +
                       "    }" +
                       ") YIELD setCount, communityCount";

        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            runQuery(query, MapUtil.map("graph", graphImpl));
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertEquals("Node properties not found: 'does_not_exist'", rootCause.getMessage());
    }

    @AllGraphNamesTest
    void testUnionFindReadAndWriteSeed(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph, seedProperty: 'seed', writeProperty: 'seed'" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                }
        );

        runQuery(
                "MATCH (n) RETURN n.seed AS partition",
                row -> assertTrue(row.getNumber("partition").longValue() >= 42)
        );

        runQuery(
                "MATCH (n) RETURN n.nodeId AS nodeId, n.seed AS partition",
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    final long partitionId = row.getNumber("partition").longValue();
                    if (nodeId >= 0 && nodeId <= 6) {
                        assertEquals(42, partitionId);
                    } else {
                        assertTrue(partitionId != 42);
                    }
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindWithSeedAndConsecutive(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph, seedProperty: 'seed', consecutiveIds: true" +
                       "    }" +
                       ") YIELD setCount, communityCount";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                }
        );

        runQuery(
                "MATCH (n) RETURN n.partition AS partition",
                row -> assertThat(row.getNumber("partition").longValue(), greaterThanOrEqualTo(42L))
        );

        runQuery(
                "MATCH (n) RETURN n.nodeId AS nodeId, n.partition AS partition",
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    final long partitionId = row.getNumber("partition").longValue();
                    if (nodeId >= 0 && nodeId <= 6) {
                        assertEquals(42, partitionId);
                    } else {
                        assertTrue(partitionId != 42);
                    }
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindWithConsecutiveIds(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph, consecutiveIds: true" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                }
        );

        runQuery(
                "MATCH (n) RETURN collect(distinct n.partition) AS partitions ",
                row -> assertThat((List<Long>) row.get("partitions"), containsInAnyOrder(0L, 1L, 2L))
        );
    }

    @AllGraphNamesTest
    void testUnionFindWriteBack(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        write: true, graph: $graph" +
                       "    }" +
                       ") YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    assertEquals("partition", row.getString("partitionProperty"));
                    assertEquals("partition", row.getString("writeProperty"));
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindWriteBackExplicitWriteProperty(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        write: true, graph: $graph, writeProperty: 'unionFind'" +
                       "    }" +
                       ") YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    assertEquals("unionFind", row.getString("partitionProperty"));
                    assertEquals("unionFind", row.getString("writeProperty"));
                }
        );
    }

    @AllGraphNamesTest
    void testUnionFindStream(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> map.addTo(row.getNumber("setId").intValue(), 1));
        assertMapContains(map, 1, 2, 7);
    }

    @AllGraphNamesTest
    void testUnionFindStreamThreshold(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        weightProperty: 'cost', defaultValue: 10.0, threshold: 5.0, concurrency: 1, graph: $graph" +
                       "    }" +
                       ") YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> map.addTo(row.getNumber("setId").intValue(), 1)
        );
        assertMapContains(map, 4, 3, 2, 1);
    }

    @AllGraphNamesTest
    void testThresholdUnionFindLowThreshold(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        weightProperty: 'cost', defaultValue: 10.0, concurrency: 1, threshold: 3.14, graph: $graph" +
                       "    }" +
                       ") YIELD setId";
        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                }
        );
        assertMapContains(map, 1, 2, 7);
    }

    @AllGraphNamesTest
    void testUnionFindThrowsExceptionWhenThresholdPropertyDoesNotExists(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph, weightProperty: 'does_not_exist', threshold: 3.14" +
                       "    }" +
                       ") YIELD setCount, communityCount";

        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            runQuery(query, MapUtil.map("graph", graphImpl), row -> {});
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertEquals("Relationship properties not found: 'does_not_exist'", rootCause.getMessage());
    }

    static Stream<Arguments> multipleReltypesAndPropertiesArguments() {
        return Stream.of(
                Arguments.of("",                "",                 "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("",                "",                 "minCost", new int[]{4, 3, 3}),
                Arguments.of("TYPE",            "",                 "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("TYPE",            "",                 "minCost", new int[]{4, 3, 2, 1}),
                Arguments.of("TYPE | TYPE_1",   "",                 "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("TYPE | TYPE_1",   "",                 "minCost", new int[]{4, 3, 3}),

                Arguments.of("TYPE | TYPE_1",   "TYPE | TYPE_1",    "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("TYPE | TYPE_1",   "TYPE | TYPE_1",    "minCost", new int[]{4, 3, 3}),

                Arguments.of("TYPE",            "TYPE",             "minCost", new int[]{4, 3, 2, 1}),
                Arguments.of("TYPE",            "TYPE",             "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("TYPE | TYPE_1",   "TYPE",             "minCost", new int[]{4, 3, 2, 1}),
                Arguments.of("TYPE | TYPE_1",   "TYPE",             "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),

                Arguments.of("TYPE_1",          "TYPE_1",           "minCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 2}),
                Arguments.of("TYPE_1",          "TYPE_1",           "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
                Arguments.of("TYPE | TYPE_1",   "TYPE_1",           "minCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 2}),
                Arguments.of("TYPE | TYPE_1",   "TYPE_1",           "maxCost", new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
        );
    }

    @ParameterizedTest(name = "loadRelType = {0}, algoRelType = {1}, weightProperty = {2}, expectedSizes = {3}")
    @MethodSource("multipleReltypesAndPropertiesArguments")
    void testUnionFindStreamFromLoadedGraph(
            String loadRelType,
            String algoRelType,
            String weightProperty,
            int[] expectedSizes) {
        String graphName = "aggGraph";
        GraphCatalog.remove(getUsername(), graphName);

        String loadQuery = "CALL algo.graph.load(" +
                           "    '" + graphName + "', '', '" + loadRelType + "', {" +
                           "        graph: 'huge'," +
                           "        relationshipProperties: {" +
                           "            minCost: {" +
                           "                property: 'cost'," +
                           "                aggregate: 'MIN'," +
                           "                defaultValue: 10.0" +
                           "            }," +
                           "            maxCost: {" +
                           "                property: 'cost'," +
                           "                aggregate: 'MAX'," +
                           "                defaultValue: 1.0" +
                           "            }" +
                           "        }" +
                           "    }" +
                           ")";

        runQuery(loadQuery);

        assertComponentSizes(graphName, algoRelType, weightProperty, expectedSizes);
    }

    private void assertComponentSizes(String graphName, String relType, String weightProperty, int[] expectedSizes) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', '" + relType +"', {" +
                       "        weightProperty: '"+ weightProperty +"', threshold: 5.0, concurrency: 1, graph: '"+ graphName +"'" +
                       "    }" +
                       ") YIELD setId";

        IntIntScatterMap map1 = new IntIntScatterMap(11);
        runQuery(query, row -> map1.addTo(row.getNumber("setId").intValue(), 1));
        assertMapContains(map1, expectedSizes);
    }

}
