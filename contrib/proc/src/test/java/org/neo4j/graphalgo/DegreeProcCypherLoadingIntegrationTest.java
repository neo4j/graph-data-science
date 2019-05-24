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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.DegreeCentralityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
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

import static org.junit.Assert.*;

public class DegreeProcCypherLoadingIntegrationTest {

    private static GraphDatabaseAPI db;
    private static Map<Long, Double> incomingExpected = new HashMap<>();
    private static Map<Long, Double> bothExpected = new HashMap<>();
    private static Map<Long, Double> outgoingExpected = new HashMap<>();
    private static Map<Long, Double> incomingWeightedExpected = new HashMap<>();
    private static Map<Long, Double> bothWeightedExpected = new HashMap<>();
    private static Map<Long, Double> outgoingWeightedExpected = new HashMap<>();
    public static String graphImpl;

    private static final String NODES = "MATCH (n) RETURN id(n) AS id";
    private static final String INCOMING_RELS = "MATCH (a)<-[r]-(b) RETURN id(a) AS source, id(b) AS target, r.foo AS weight";
    private static final String BOTH_RELS = "MATCH (a)-[r]-(b) RETURN id(a) AS source, id(b) AS target, r.foo AS weight";
    private static final String OUTGOING_RELS = "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.foo AS weight";

    private static final String DB_CYPHER = "" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE\n" +
            "  (a)-[:TYPE1{foo:3.0}]->(b),\n" +
            "  (b)-[:TYPE1{foo:5.0}]->(c),\n" +
            "  (a)-[:TYPE1{foo:2.1}]->(c),\n" +
            "  (a)-[:TYPE2{foo:7.1}]->(c)\n";


    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @BeforeClass
    public static void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }

        graphImpl = "cypher";

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(DegreeCentralityProc.class);


        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Label1");
            incomingExpected.put(db.findNode(label, "name", "a").getId(), 0.0);
            incomingExpected.put(db.findNode(label, "name", "b").getId(), 1.0);
            incomingExpected.put(db.findNode(label, "name", "c").getId(), 2.0);

            incomingWeightedExpected.put(db.findNode(label, "name", "a").getId(), 0.0);
            incomingWeightedExpected.put(db.findNode(label, "name", "b").getId(), 3.0);
            incomingWeightedExpected.put(db.findNode(label, "name", "c").getId(), 14.2);

            bothExpected.put(db.findNode(label, "name", "a").getId(), 2.0);
            bothExpected.put(db.findNode(label, "name", "b").getId(), 2.0);
            bothExpected.put(db.findNode(label, "name", "c").getId(), 2.0);

            bothWeightedExpected.put(db.findNode(label, "name", "a").getId(), 12.2);
            bothWeightedExpected.put(db.findNode(label, "name", "b").getId(), 8.0);
            bothWeightedExpected.put(db.findNode(label, "name", "c").getId(), 14.2);

            outgoingExpected.put(db.findNode(label, "name", "a").getId(), 2.0);
            outgoingExpected.put(db.findNode(label, "name", "b").getId(), 1.0);
            outgoingExpected.put(db.findNode(label, "name", "c").getId(), 0.0);

            outgoingWeightedExpected.put(db.findNode(label, "name", "a").getId(), 12.2);
            outgoingWeightedExpected.put(db.findNode(label, "name", "b").getId(), 5.0);
            outgoingWeightedExpected.put(db.findNode(label, "name", "c").getId(), 0.0);

            tx.success();
        }
    }

    @Test
    public void testDegreeIncomingStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.degree.stream($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'INCOMING', duplicateRelationships:'skip'}) YIELD nodeId, score",
                MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(incomingExpected, actual);
    }

    @Test
    public void testWeightedDegreeIncomingStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.degree.stream($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'INCOMING', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD nodeId, score",
                MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(incomingWeightedExpected, actual);
    }

    @Test
    public void testDegreeIncomingWriteBack() throws Exception {
        runQuery(
                "CALL algo.degree($nodeQuery, $relQuery,  {graph:'"+graphImpl+"', direction:'INCOMING', duplicateRelationships:'skip'}) YIELD writeMillis, write, writeProperty",
                MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("degree", incomingExpected);
    }

    @Test
    public void testWeightedDegreeIncomingWriteBack() throws Exception {
        runQuery(
                "CALL algo.degree($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'INCOMING', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD writeMillis, write, writeProperty",
                MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("degree", incomingWeightedExpected);
    }

    @Test
    public void testDegreeBothStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.degree.stream($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'BOTH', duplicateRelationships:'skip'}) YIELD nodeId, score",
                MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(bothExpected, actual);
    }

    @Test
    public void testWeightedDegreeBothStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.degree.stream($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'BOTH', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD nodeId, score",
                MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(bothWeightedExpected, actual);
    }

    @Test
    public void testDegreeBothWriteBack() throws Exception {
        runQuery(
                "CALL algo.degree($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'BOTH', duplicateRelationships:'skip'}) YIELD writeMillis, write, writeProperty",
                MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("degree", bothExpected);
    }

    @Test
    public void testWeightedDegreeBothWriteBack() throws Exception {
        runQuery(
                "CALL algo.degree($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'BOTH', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD writeMillis, write, writeProperty",
                MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("degree", bothWeightedExpected);
    }

    @Test
    public void testDegreeOutgoingStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.degree.stream($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'OUTGOING', duplicateRelationships:'skip'}) YIELD nodeId, score",
                MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(outgoingExpected, actual);
    }

    @Test
    public void testWeightedDegreeOutgoingStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.degree.stream($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'OUTGOING', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD nodeId, score",
                MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(outgoingWeightedExpected, actual);
    }

    @Test
    public void testDegreeOutgoingWriteBack() throws Exception {
        runQuery(
                "CALL algo.degree($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'OUTGOING', duplicateRelationships:'skip'}) YIELD writeMillis, write, writeProperty",
                MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("degree", outgoingExpected);
    }

    @Test
    public void testWeightedDegreeOutgoingWriteBack() throws Exception {
        runQuery(
                "CALL algo.degree($nodeQuery, $relQuery, {graph:'"+graphImpl+"', direction:'OUTGOING', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD writeMillis, write, writeProperty",
                MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("degree", outgoingWeightedExpected);
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
