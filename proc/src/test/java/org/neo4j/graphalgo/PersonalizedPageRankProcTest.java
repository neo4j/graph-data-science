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
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

class PersonalizedPageRankProcTest {

    private static final Map<Long, Double> EXPECTED = new HashMap<>();

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (d:Label1 {name: 'd'})" +
            ", (e:Label1 {name: 'e'})" +
            ", (f:Label1 {name: 'f'})" +
            ", (g:Label1 {name: 'g'})" +
            ", (h:Label1 {name: 'h'})" +
            ", (i:Label1 {name: 'i'})" +
            ", (j:Label1 {name: 'j'})" +
            ", (k:Label2 {name: 'k'})" +
            ", (l:Label2 {name: 'l'})" +
            ", (m:Label2 {name: 'm'})" +
            ", (n:Label2 {name: 'n'})" +
            ", (o:Label2 {name: 'o'})" +
            ", (p:Label2 {name: 'p'})" +
            ", (q:Label2 {name: 'q'})" +
            ", (r:Label2 {name: 'r'})" +
            ", (s:Label2 {name: 's'})" +
            ", (t:Label2 {name: 't'})" +

            ", (b)-[:TYPE1{foo:1.0}]->(c)" +
            ", (c)-[:TYPE1{foo:1.2}]->(b)" +
            ", (d)-[:TYPE1{foo:1.3}]->(a)" +
            ", (d)-[:TYPE1{foo:1.7}]->(b)" +
            ", (e)-[:TYPE1{foo:1.1}]->(b)" +
            ", (e)-[:TYPE1{foo:2.2}]->(d)" +
            ", (e)-[:TYPE1{foo:1.5}]->(f)" +
            ", (f)-[:TYPE1{foo:3.5}]->(b)" +
            ", (f)-[:TYPE1{foo:2.9}]->(e)" +
            ", (g)-[:TYPE2{foo:3.2}]->(b)" +
            ", (g)-[:TYPE2{foo:5.3}]->(e)" +
            ", (h)-[:TYPE2{foo:9.5}]->(b)" +
            ", (h)-[:TYPE2{foo:0.3}]->(e)" +
            ", (i)-[:TYPE2{foo:5.4}]->(b)" +
            ", (i)-[:TYPE2{foo:3.2}]->(e)" +
            ", (j)-[:TYPE2{foo:9.5}]->(e)" +
            ", (k)-[:TYPE2{foo:4.2}]->(e)";

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setup() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(DB_CYPHER);
        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(PageRankProc.class);

        try (Transaction tx = DB.beginTx()) {
            final Label label = Label.label("Label1");
            EXPECTED.put(DB.findNode(label, "name", "a").getId(), 0.243);
            EXPECTED.put(DB.findNode(label, "name", "b").getId(), 1.844);
            EXPECTED.put(DB.findNode(label, "name", "c").getId(), 1.777);
            EXPECTED.put(DB.findNode(label, "name", "d").getId(), 0.218);
            EXPECTED.put(DB.findNode(label, "name", "e").getId(), 0.243);
            EXPECTED.put(DB.findNode(label, "name", "f").getId(), 0.218);
            EXPECTED.put(DB.findNode(label, "name", "g").getId(), 0.150);
            EXPECTED.put(DB.findNode(label, "name", "h").getId(), 0.150);
            EXPECTED.put(DB.findNode(label, "name", "i").getId(), 0.150);
            EXPECTED.put(DB.findNode(label, "name", "j").getId(), 0.150);
            tx.success();
        }
    }

    @AfterAll
    static void tearDown() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphNamesTest
    void testPageRankStream(String graphName) {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.pageRank.stream('Label1', 'TYPE1', {graph:'" + graphName + "'}) YIELD nodeId, score",
                row -> actual.put(
                        (Long) row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(actual);
    }

    @AllGraphNamesTest
    void testPageRankWriteBack(String graphName) {
        runQuery(
                "CALL algo.pageRank('Label1', 'TYPE1', {graph:'" + graphName + "'}) YIELD writeMillis, write, writeProperty",
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("pagerank", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("pagerank");
    }

    @AllGraphNamesTest
    void testPageRankWriteBackUnderDifferentProperty(String graphName) {
        runQuery(
                "CALL algo.pageRank('Label1', 'TYPE1', {writeProperty:'foobar', graph:'" + graphName + "'}) YIELD writeMillis, write, writeProperty",
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("foobar", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("foobar");
    }

    @AllGraphNamesTest
    void testPageRankParallelWriteBack(String graphName) {
        runQuery(
                "CALL algo.pageRank('Label1', 'TYPE1', {batchSize:3, write:true, graph:'" + graphName + "'}) YIELD writeMillis, write, writeProperty",
                row -> assertTrue(
                        "write time not set",
                        row.getNumber("writeMillis").intValue() >= 0));

        assertResult("pagerank");
    }

    @AllGraphNamesTest
    void testPageRankParallelExecution(String graphName) {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.pageRank.stream('Label1', 'TYPE1', {batchSize:2, graph:'" + graphName + "'}) YIELD nodeId, score",
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    actual.put(nodeId, (Double) row.get("score"));
                });
        assertMapEquals(actual);
    }

    private static void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        runQuery(query, new HashMap<>(), check);
    }

    private static void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = DB.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    private void assertResult(final String scoreProperty) {
        try (Transaction tx = DB.beginTx()) {
            for (Map.Entry<Long, Double> entry : EXPECTED.entrySet()) {
                double score = ((Number) DB
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

    private static void assertMapEquals(Map<Long, Double> actual) {
        assertEquals("number of elements", EXPECTED.size(), actual.size());
        Set<Long> expectedKeys = new HashSet<>(EXPECTED.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                    "unknown key " + entry.getKey(),
                    expectedKeys.remove(entry.getKey()));
            assertEquals(
                    "value for " + entry.getKey(),
                    EXPECTED.get(entry.getKey()),
                    entry.getValue(),
                    0.1);
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }
}
