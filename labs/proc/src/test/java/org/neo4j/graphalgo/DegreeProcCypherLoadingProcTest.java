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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DegreeProcCypherLoadingProcTest extends ProcTestBase {

    private static final Map<Long, Double> incomingExpected = new HashMap<>();
    private static final Map<Long, Double> bothExpected = new HashMap<>();
    private static final Map<Long, Double> outgoingExpected = new HashMap<>();
    private static final Map<Long, Double> incomingWeightedExpected = new HashMap<>();
    private static final Map<Long, Double> bothWeightedExpected = new HashMap<>();
    private static final Map<Long, Double> outgoingWeightedExpected = new HashMap<>();
    public static String graphImpl;

    private static final String NODES = "MATCH (n) RETURN id(n) AS id";
    private static final String INCOMING_RELS = "MATCH (a)<-[r]-(b) RETURN id(a) AS source, id(b) AS target, r.foo AS weight";
    private static final String BOTH_RELS = "MATCH (a)-[r]-(b) RETURN id(a) AS source, id(b) AS target, r.foo AS weight";
    private static final String OUTGOING_RELS = "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.foo AS weight";

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1 {name: 'a'})" +
            ", (b:Label1 {name: 'b'})" +
            ", (c:Label1 {name: 'c'})" +
            ", (a)-[:TYPE1 {foo: 3.0}]->(b)" +
            ", (b)-[:TYPE1 {foo: 5.0}]->(c)" +
            ", (a)-[:TYPE1 {foo: 2.1}]->(c)" +
            ", (a)-[:TYPE2 {foo: 7.1}]->(c)";


    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            runQuery(DB_CYPHER);
            tx.success();
        }

        graphImpl = "cypher";

        registerProcedures(DegreeCentralityProc.class);

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
    void testDegreeIncomingStream() {
        final Map<Long, Double> actual = new HashMap<>();

        String query = "CALL algo.degree.stream(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'INCOMING', duplicateRelationships: 'skip'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(incomingExpected, actual);
    }

    @Test
    void testWeightedDegreeIncomingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'INCOMING', weightProperty: 'foo', duplicateRelationships: 'sum'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(incomingWeightedExpected, actual);
    }

    @Test
    void testDegreeIncomingWriteBack() {
        String query = "CALL algo.degree(" +
                       "    $nodeQuery," +
                       "    $relQuery,  {" +
                       "        graph: $graph, direction: 'INCOMING', duplicateRelationships: 'skip'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                });
        assertResult("degree", incomingExpected);
    }

    @Test
    void testWeightedDegreeIncomingWriteBack() {
        String query = "CALL algo.degree(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'INCOMING', weightProperty: 'foo', duplicateRelationships: 'sum'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", INCOMING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                });
        assertResult("degree", incomingWeightedExpected);
    }

    @Test
    void testDegreeBothStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'BOTH', duplicateRelationships: 'skip'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(bothExpected, actual);
    }

    @Test
    void testWeightedDegreeBothStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    $nodeQuery, $relQuery, {" +
                       "        graph: $graph, direction: 'BOTH', weightProperty: 'foo', duplicateRelationships: 'sum'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", BOTH_RELS, "graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(bothWeightedExpected, actual);
    }

    @Test
    void testDegreeBothWriteBack() {
        String query = "CALL algo.degree(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'BOTH', duplicateRelationships: 'skip'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                });
        assertResult("degree", bothExpected);
    }

    @Test
    void testWeightedDegreeBothWriteBack() {
        String query = "CALL algo.degree(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'BOTH', weightProperty: 'foo', duplicateRelationships: 'sum'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", BOTH_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                });
        assertResult("degree", bothWeightedExpected);
    }

    @Test
    void testDegreeOutgoingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    $nodeQuery," +
                       "    $relQuery, {" +
                       "        graph: $graph, direction: 'OUTGOING', duplicateRelationships: 'skip'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(outgoingExpected, actual);
    }

    @Test
    void testWeightedDegreeOutgoingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query= "CALL algo.degree.stream($nodeQuery, $relQuery, {graph: $graph, direction:'OUTGOING', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(outgoingWeightedExpected, actual);
    }

    @Test
    void testDegreeOutgoingWriteBack() {
        String query = "CALL algo.degree($nodeQuery, $relQuery, {graph: $graph, direction:'OUTGOING', duplicateRelationships:'skip'}) YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                });
        assertResult("degree", outgoingExpected);
    }

    @Test
    void testWeightedDegreeOutgoingWriteBack() {
        String query = "CALL algo.degree($nodeQuery, $relQuery, {graph: $graph, direction:'OUTGOING', weightProperty: 'foo', duplicateRelationships:'sum'}) YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl, "nodeQuery", NODES, "relQuery", OUTGOING_RELS),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                });
        assertResult("degree", outgoingWeightedExpected);
    }
}
