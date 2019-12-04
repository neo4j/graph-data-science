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
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.wcc.WccProc;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.ProcedureConstants.DEPRECATED_RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_WEIGHT_KEY;
import static org.neo4j.graphalgo.impl.wcc.WCCFactory.CONFIG_THRESHOLD;

class WccBetaProcTest extends ProcTestBase {

    @BeforeEach
    void setup() throws Exception {
        String createGraph = "CREATE" +
                             " (nA:Label {nodeId: 0, seedId: 42})" +
                             ",(nB:Label {nodeId: 1, seedId: 42})" +
                             ",(nC:Label {nodeId: 2, seedId: 42})" +
                             ",(nD:Label {nodeId: 3, seedId: 42})" +
                             ",(nE {nodeId: 4})" +
                             ",(nF {nodeId: 5})" +
                             ",(nG {nodeId: 6})" +
                             ",(nH {nodeId: 7})" +
                             ",(nI {nodeId: 8})" +
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
                             ",(nH)-[:TYPE]->(nI)";

        db = TestDatabaseCreator.createTestDatabase();
        runQuery(createGraph);
        registerProcedures(WccProc.class, GraphLoadProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphNamesTest
    void testWCC(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', '', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                assertEquals(3L, row.getNumber("communityCount"));
                assertEquals(3L, row.getNumber("setCount"));
            }
        );
    }

    @AllGraphNamesTest
    void testWCCWithLabel(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    'Label', '', {" +
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
    void testWCCWithSeed(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', '', {" +
                       "        graph: $graph, seedProperty: 'seedId'" +
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
    void testWCCWithSeedOnPreloadedGraph(String graphImpl) {
        String loadQuery = "CALL algo.graph.load('seedGraph', '', '', {" +
                           "    graph: $graph," +
                           "    nodeProperties: {" +
                           "        seedId: 'seedId'" +
                           "    }" +
                           "})";
        runQuery(loadQuery, MapUtil.map("graph", graphImpl));
        String query = "CALL algo.beta.wcc(" +
                       "    '', '', {" +
                       "        graph: 'seedGraph', seedProperty: 'seedId'" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(
            query,
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
    void testWCCReadAndWriteSeed(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', '', {" +
                       "        graph: $graph, seedProperty: 'seedId', writeProperty: 'seedId'" +
                       "    }" +
                       ") YIELD setCount, communityCount";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                assertEquals(3L, row.getNumber("communityCount"));
                assertEquals(3L, row.getNumber("setCount"));
            }
        );

        runQuery(
            "MATCH (n) RETURN n.seedId AS partition",
            row -> assertTrue(row.getNumber("partition").longValue() >= 42)
        );

        runQuery(
            "MATCH (n) RETURN n.nodeId AS nodeId, n.seedId AS partition",
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
    void testWCCWithSeedAndConsecutive(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', '', {" +
                       "        graph: $graph, seedProperty: 'seedId', consecutiveIds: true" +
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
    void testWCCWithConsecutiveIds(String graphImpl) {
        String query = "CALL algo.beta.wcc(" +
                       "    '', '', {" +
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
    void testWCCWriteBack(String graphImpl) {
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
    void testWCCWriteBackExplicitWriteProperty(String graphImpl) {
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
    void testWCCStream(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> map.addTo(row.getNumber("setId").intValue(), 1)
        );
        assertMapContains(map, 1, 2, 7);
    }

    @AllGraphNamesTest
    void testThresholdWCCStream(String graphImpl) {
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
    void testThresholdWCCLowThreshold(String graphImpl) {
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
    void shouldFailWhenSpecifyingThresholdWithoutRelationshipWeight(String graphImpl) {
        String query = "CALL algo.beta.wcc.stream(" +
                       "    '', 'TYPE', {" +
                       "        defaultValue: 10.0, concurrency: 1, threshold: 3.14, graph: $graph" +
                       "    }" +
                       ") YIELD setId";
        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query, MapUtil.map("graph", graphImpl))
        );
        Throwable rootCause = ExceptionUtil.rootCause(exception);

        assertTrue(rootCause
            .getMessage()
            .contains(String.format(
                "%s requires a `%s` or `%s`",
                CONFIG_THRESHOLD,
                RELATIONSHIP_WEIGHT_KEY,
                DEPRECATED_RELATIONSHIP_PROPERTY_KEY
            )));
    }

    @AllGraphNamesTest
    void testUnionFindPregel(String graphImpl) {
        String query = "CALL algo.beta.wcc.pregel(" +
                       "    '', 'TYPE', {" +
                       "        graph: $graph" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                assertEquals(3L, row.getNumber("communityCount"));
                assertEquals(3L, row.getNumber("setCount"));
            }
        );
    }
}
