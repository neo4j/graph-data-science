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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PageRankProcIntegrationTest {

    private static GraphDatabaseAPI db;
    private static Map<Long, Double> expected = new HashMap<>();
    private static Map<Long, Double> weightedExpected = new HashMap<>();

    private static final String DB_CYPHER = "CREATE " +
            " (a:Label1 {name: 'a'})" +
            ",(b:Label1 {name: 'b'})" +
            ",(c:Label1 {name: 'c'})" +
            ",(d:Label1 {name: 'd'})" +
            ",(e:Label1 {name: 'e'})" +
            ",(f:Label1 {name: 'f'})" +
            ",(g:Label1 {name: 'g'})" +
            ",(h:Label1 {name: 'h'})" +
            ",(i:Label1 {name: 'i'})" +
            ",(j:Label1 {name: 'j'})" +
            ",(k:Label2 {name: 'k'})" +
            ",(l:Label2 {name: 'l'})" +
            ",(m:Label2 {name: 'm'})" +
            ",(n:Label2 {name: 'n'})" +
            ",(o:Label2 {name: 'o'})" +
            ",(p:Label2 {name: 'p'})" +
            ",(q:Label2 {name: 'q'})" +
            ",(r:Label2 {name: 'r'})" +
            ",(s:Label2 {name: 's'})" +
            ",(t:Label2 {name: 't'})" +
            ",(b)-[:TYPE1 {foo: 1.0,  equalWeight: 1.0}]->(c)" +
            ",(c)-[:TYPE1 {foo: 1.2,  equalWeight: 1.0}]->(b)" +
            ",(d)-[:TYPE1 {foo: 1.3,  equalWeight: 1.0}]->(a)" +
            ",(d)-[:TYPE1 {foo: 1.7,  equalWeight: 1.0}]->(b)" +
            ",(e)-[:TYPE1 {foo: 6.1,  equalWeight: 1.0}]->(b)" +
            ",(e)-[:TYPE1 {foo: 2.2,  equalWeight: 1.0}]->(d)" +
            ",(e)-[:TYPE1 {foo: 1.5,  equalWeight: 1.0}]->(f)" +
            ",(f)-[:TYPE1 {foo: 10.5, equalWeight: 1.0}]->(b)" +
            ",(f)-[:TYPE1 {foo: 2.9,  equalWeight: 1.0}]->(e)" +
            ",(g)-[:TYPE2 {foo: 3.2,  equalWeight: 1.0}]->(b)" +
            ",(g)-[:TYPE2 {foo: 5.3,  equalWeight: 1.0}]->(e)" +
            ",(h)-[:TYPE2 {foo: 9.5,  equalWeight: 1.0}]->(b)" +
            ",(h)-[:TYPE2 {foo: 0.3,  equalWeight: 1.0}]->(e)" +
            ",(i)-[:TYPE2 {foo: 5.4,  equalWeight: 1.0}]->(b)" +
            ",(i)-[:TYPE2 {foo: 3.2,  equalWeight: 1.0}]->(e)" +
            ",(j)-[:TYPE2 {foo: 9.5,  equalWeight: 1.0}]->(e)" +
            ",(k)-[:TYPE2 {foo: 4.2,  equalWeight: 1.0}]->(e)";

    @AfterAll
    public static void tearDown() {
        if (db != null) db.shutdown();
    }

    @BeforeAll
    public static void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(PageRankProc.class);

        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Label1");
            expected.put(db.findNode(label, "name", "a").getId(), 0.243);
            expected.put(db.findNode(label, "name", "b").getId(), 1.844);
            expected.put(db.findNode(label, "name", "c").getId(), 1.777);
            expected.put(db.findNode(label, "name", "d").getId(), 0.218);
            expected.put(db.findNode(label, "name", "e").getId(), 0.243);
            expected.put(db.findNode(label, "name", "f").getId(), 0.218);
            expected.put(db.findNode(label, "name", "g").getId(), 0.150);
            expected.put(db.findNode(label, "name", "h").getId(), 0.150);
            expected.put(db.findNode(label, "name", "i").getId(), 0.150);
            expected.put(db.findNode(label, "name", "j").getId(), 0.150);

            weightedExpected.put(db.findNode(label, "name", "a").getId(), 0.218);
            weightedExpected.put(db.findNode(label, "name", "b").getId(), 2.008);
            weightedExpected.put(db.findNode(label, "name", "c").getId(), 1.850);
            weightedExpected.put(db.findNode(label, "name", "d").getId(), 0.185);
            weightedExpected.put(db.findNode(label, "name", "e").getId(), 0.182);
            weightedExpected.put(db.findNode(label, "name", "f").getId(), 0.174);
            weightedExpected.put(db.findNode(label, "name", "g").getId(), 0.150);
            weightedExpected.put(db.findNode(label, "name", "h").getId(), 0.150);
            weightedExpected.put(db.findNode(label, "name", "i").getId(), 0.150);
            weightedExpected.put(db.findNode(label, "name", "j").getId(), 0.150);
            tx.success();
        }
    }

    static Stream<String> testArguments() {
        return Stream.of("Heavy", "Huge", "Kernel");
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPageRankStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "         graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testWeightedPageRankStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testWeightedPageRankWithCachedWeightsStream(String graphImpl) throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'foo', cacheWeights: true" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testWeightedPageRankWithAllRelationshipsEqualStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'equalWeight'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }


    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPageRankWriteBack(String graphImpl) {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("pagerank", row.getString("writeProperty"));
                    assertTrue("write time not set", row.getNumber("writeMillis").intValue() >= 0);
                }
        );
        assertResult("pagerank", expected);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testWeightedPageRankWriteBack(String graphImpl) {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("pagerank", row.getString("writeProperty"));
                    assertTrue("write time not set", row.getNumber("writeMillis").intValue() >= 0);
                }
        );
        assertResult("pagerank", weightedExpected);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPageRankWriteBackUnderDifferentProperty(String graphImpl) throws Exception {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        writeProperty: 'foobar', graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("foobar", row.getString("writeProperty"));
                    assertTrue("write time not set", row.getNumber("writeMillis").intValue() >= 0);
                }
        );
        assertResult("foobar", expected);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPageRankParallelWriteBack(String graphImpl) throws Exception {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        batchSize: 3, write: true, graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> assertTrue("write time not set", row.getNumber("writeMillis").intValue() >= 0)
        );
        assertResult("pagerank", expected);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPageRankParallelExecution(String graphImpl) throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        batchSize: 2, graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    actual.put(nodeId, (Double) row.get("score"));
                }
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPageRankWithToleranceParam(String graphImpl) throws Exception {
        String query;
        query = "CALL algo.pageRank(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 0.0001, batchSize: 3, graph: $graph" +
                "     }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> assertEquals(20L, (long) row.getNumber("iterations")));
        query = "CALL algo.pageRank(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 100.0, batchSize: 3, graph: $graph" +
                "    }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> assertEquals(1L, (long)row.getNumber("iterations")));
        query = "CALL algo.pageRank(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 0.20010237991809848, batchSize: 3, graph: $graph" +
                "    }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> assertEquals(4L, (long) row.getNumber("iterations")));
        query = "CALL algo.pageRank(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 0.20010237991809843, batchSize: 3, graph: $graph" +
                "    }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> assertEquals(5L, (long) row.getNumber("iterations")));
    }

    private static void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    private void assertResult(final String scoreProperty, Map<Long, Double> expected) {
        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) db
                        .getNodeById(entry.getKey())
                        .getProperty(scoreProperty)).doubleValue();
                assertEquals(
                        "score for " + entry.getKey(),
                        entry.getValue(),
                        score,
                        0.1);
            }
            tx.success();
        }
    }

    private static void assertMapEquals(
            Map<Long, Double> expected,
            Map<Long, Double> actual) {
        assertEquals("number of elements", expected.size(), actual.size());
        HashSet<Long> expectedKeys = new HashSet<>(expected.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                    "unknown key " + entry.getKey(),
                    expectedKeys.remove(entry.getKey()));
            assertEquals(
                    "value for " + entry.getKey(),
                    expected.get(entry.getKey()),
                    entry.getValue(),
                    0.1);
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }
}
