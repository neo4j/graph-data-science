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
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;

class DegreeCentralityProcTest extends BaseProcTest {

    private static final Map<Long, Double> incomingExpected = new HashMap<>();
    private static final Map<Long, Double> bothExpected = new HashMap<>();
    private static final Map<Long, Double> outgoingExpected = new HashMap<>();
    private static final Map<Long, Double> incomingWeightedExpected = new HashMap<>();
    private static final Map<Long, Double> bothWeightedExpected = new HashMap<>();
    private static final Map<Long, Double> outgoingWeightedExpected = new HashMap<>();

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
        runQuery(DB_CYPHER);
        registerProcedures(DegreeCentralityProc.class);

        runInTransaction(db, () -> {
            Label label = Label.label("Label1");
            incomingExpected.put(db.findNode(label, "name", "a").getId(), 0.0);
            incomingExpected.put(db.findNode(label, "name", "b").getId(), 1.0);
            incomingExpected.put(db.findNode(label, "name", "c").getId(), 2.0);

            incomingWeightedExpected.put(db.findNode(label, "name", "a").getId(), 0.0);
            incomingWeightedExpected.put(db.findNode(label, "name", "b").getId(), 3.0);
            incomingWeightedExpected.put(db.findNode(label, "name", "c").getId(), 7.1);

            bothExpected.put(db.findNode(label, "name", "a").getId(), 2.0);
            bothExpected.put(db.findNode(label, "name", "b").getId(), 2.0);
            bothExpected.put(db.findNode(label, "name", "c").getId(), 2.0);

            bothWeightedExpected.put(db.findNode(label, "name", "a").getId(), 5.1);
            bothWeightedExpected.put(db.findNode(label, "name", "b").getId(), 8.0);
            bothWeightedExpected.put(db.findNode(label, "name", "c").getId(), 7.1);

            outgoingExpected.put(db.findNode(label, "name", "a").getId(), 2.0);
            outgoingExpected.put(db.findNode(label, "name", "b").getId(), 1.0);
            outgoingExpected.put(db.findNode(label, "name", "c").getId(), 0.0);

            outgoingWeightedExpected.put(db.findNode(label, "name", "a").getId(), 5.1);
            outgoingWeightedExpected.put(db.findNode(label, "name", "b").getId(), 5.0);
            outgoingWeightedExpected.put(db.findNode(label, "name", "c").getId(), 0.0);
        });
    }

    @AllGraphNamesTest
    public void testDegreeIncomingStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'INCOMING'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(incomingExpected, actual);
    }

    @AllGraphNamesTest
    public void testWeightedDegreeIncomingStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'INCOMING', weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(incomingWeightedExpected, actual);
    }

    @AllGraphNamesTest
    public void testDegreeIncomingWriteBack(String graphImpl) {
        String query = "CALL algo.degree(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'INCOMING'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                }
        );
        assertResult("degree", incomingExpected);
    }

    @AllGraphNamesTest
    public void testWeightedDegreeIncomingWriteBack(String graphImpl) {
        String query = "CALL algo.degree(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'INCOMING', weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(
                            row.getNumber("writeMillis").intValue() >= 0,
                            "write time not set");
                }
        );
        assertResult("degree", incomingWeightedExpected);
    }

    @AllGraphNamesTest
    public void testDegreeBothStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'BOTH'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(bothExpected, actual);
    }

    @AllGraphNamesTest
    public void testWeightedDegreeBothStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'BOTH', weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(bothWeightedExpected, actual);
    }

    @AllGraphNamesTest
    public void testDegreeBothWriteBack(String graphImpl) {
        String query = "CALL algo.degree(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'BOTH'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("degree", bothExpected);
    }

    @AllGraphNamesTest
    public void testWeightedDegreeBothWriteBack(String graphImpl) {
        String query = "CALL algo.degree(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'BOTH', weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("degree", bothWeightedExpected);
    }

    @AllGraphNamesTest
    public void testDegreeOutgoingStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'OUTGOING'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingExpected, actual);
    }

    @AllGraphNamesTest
    public void testWeightedDegreeOutgoingStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'OUTGOING', weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingWeightedExpected, actual);
    }

    @AllGraphNamesTest
    public void testDegreeOutgoingWriteBack(String graphImpl) {
        String query = "CALL algo.degree(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'OUTGOING'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("degree", outgoingExpected);
    }

    @AllGraphNamesTest
    public void testWeightedDegreeOutgoingWriteBack(String graphImpl) {
        String query = "CALL algo.degree(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, direction: 'OUTGOING', weightProperty: 'foo'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("degree", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("degree", outgoingWeightedExpected);
    }
}
