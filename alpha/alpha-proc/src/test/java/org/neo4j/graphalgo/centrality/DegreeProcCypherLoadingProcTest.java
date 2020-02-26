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
package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.findNode;

class DegreeProcCypherLoadingProcTest extends BaseProcTest {

    private static final Map<Long, Double> incomingExpected = new HashMap<>();
    private static final Map<Long, Double> bothExpected = new HashMap<>();
    private static final Map<Long, Double> outgoingExpected = new HashMap<>();
    private static final Map<Long, Double> incomingWeightedExpected = new HashMap<>();
    private static final Map<Long, Double> bothWeightedExpected = new HashMap<>();
    private static final Map<Long, Double> outgoingWeightedExpected = new HashMap<>();

    private static final String NODES = "MATCH (n) RETURN id(n) AS id";
    private static final String INCOMING_RELS = "MATCH (a)<-[r]-(b) RETURN id(a) AS source, id(b) AS target, r.foo AS foo";
    private static final String BOTH_RELS = "MATCH (a)-[r]-(b) RETURN id(a) AS source, id(b) AS target, r.foo AS foo";
    private static final String OUTGOING_RELS = "MATCH (a)-[r]->(b) RETURN id(a) AS source, id(b) AS target, r.foo AS foo";

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

        runInTransaction(db, tx -> {
            Label label = Label.label("Label1");
            incomingExpected.put(findNode(db, tx, label, "name", "a").getId(), 0.0);
            incomingExpected.put(findNode(db, tx, label, "name", "b").getId(), 1.0);
            incomingExpected.put(findNode(db, tx, label, "name", "c").getId(), 2.0);

            incomingWeightedExpected.put(findNode(db, tx, label, "name", "a").getId(), 0.0);
            incomingWeightedExpected.put(findNode(db, tx, label, "name", "b").getId(), 3.0);
            incomingWeightedExpected.put(findNode(db, tx, label, "name", "c").getId(), 14.2);

            bothExpected.put(findNode(db, tx, label, "name", "a").getId(), 2.0);
            bothExpected.put(findNode(db, tx, label, "name", "b").getId(), 2.0);
            bothExpected.put(findNode(db, tx, label, "name", "c").getId(), 2.0);

            bothWeightedExpected.put(findNode(db, tx, label, "name", "a").getId(), 12.2);
            bothWeightedExpected.put(findNode(db, tx, label, "name", "b").getId(), 8.0);
            bothWeightedExpected.put(findNode(db, tx, label, "name", "c").getId(), 14.2);

            outgoingExpected.put(findNode(db, tx, label, "name", "a").getId(), 2.0);
            outgoingExpected.put(findNode(db, tx, label, "name", "b").getId(), 1.0);
            outgoingExpected.put(findNode(db, tx, label, "name", "c").getId(), 0.0);

            outgoingWeightedExpected.put(findNode(db, tx, label, "name", "a").getId(), 12.2);
            outgoingWeightedExpected.put(findNode(db, tx, label, "name", "b").getId(), 5.0);
            outgoingWeightedExpected.put(findNode(db, tx, label, "name", "c").getId(), 0.0);
        });
    }

    @Test
    void testDegreeIncomingStream() {
        Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'single'" +
                       "        }" +
                       "    }" +
                       "}) YIELD nodeId, score";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(incomingExpected, actual);
    }

    @Test
    void testWeightedDegreeIncomingStream() {
        Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'sum'" +
                       "        }" +
                       "    }," +
                       "    relationshipWeightProperty: 'foo'" +
                       "}) YIELD nodeId, score";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(incomingWeightedExpected, actual);
    }

    @Test
    void testDegreeIncomingWriteBack() {
        String query = "CALL gds.alpha.degree.write({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'single'" +
                       "        }" +
                       "    }" +
                       "}) YIELD writeMillis, writeProperty";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
            row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", incomingExpected);
    }

    @Test
    void testWeightedDegreeIncomingWriteBack() {
        String query = "CALL gds.alpha.degree.write({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'sum'" +
                       "        }" +
                       "    }," +
                       "    relationshipWeightProperty: 'foo'" +
                       "}) YIELD writeMillis, writeProperty";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", INCOMING_RELS),
            row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", incomingWeightedExpected);
    }

    @Test
    void testDegreeBothStream() {
        Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'single'" +
                       "        }" +
                       "    }" +
                       "}) YIELD nodeId, score";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(bothExpected, actual);
    }

    @Test
    void testWeightedDegreeBothStream() {
        Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'sum'" +
                       "        }" +
                       "    }," +
                       "    relationshipWeightProperty: 'foo'" +
                       "}) YIELD nodeId, score";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(bothWeightedExpected, actual);
    }

    @Test
    void testDegreeBothWriteBack() {
        String query = "CALL gds.alpha.degree.write({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'single'" +
                       "        }" +
                       "    }" +
                       "}) YIELD writeMillis, writeProperty";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
            row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", bothExpected);
    }

    @Test
    void testWeightedDegreeBothWriteBack() {
        String query = "CALL gds.alpha.degree.write({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'sum'" +
                       "        }" +
                       "    }," +
                       "    relationshipWeightProperty: 'foo'" +
                       "}) YIELD writeMillis, writeProperty";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", BOTH_RELS),
            row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", bothWeightedExpected);
    }

    @Test
    void testDegreeOutgoingStream() {
        Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'single'" +
                       "        }" +
                       "    }" +
                       "}) YIELD nodeId, score";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingExpected, actual);
    }

    @Test
    void testWeightedDegreeOutgoingStream() {
        Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'sum'" +
                       "        }" +
                       "    }," +
                       "    relationshipWeightProperty: 'foo'" +
                       "}) YIELD nodeId, score";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingWeightedExpected, actual);
    }

    @Test
    void testDegreeOutgoingWriteBack() {
        String query = "CALL gds.alpha.degree.write({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'single'" +
                       "        }" +
                       "    }" +
                       "}) YIELD writeMillis, writeProperty";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
            row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", outgoingExpected);
    }

    @Test
    void testWeightedDegreeOutgoingWriteBack() {
        String query = "CALL gds.alpha.degree.write({" +
                       "    nodeQuery: $nodeQuery," +
                       "    relationshipQuery: $relQuery," +
                       "    relationshipProperties: {" +
                       "        foo: {" +
                       "            property: 'foo'," +
                       "            aggregation: 'sum'" +
                       "        }" +
                       "    }," +
                       "    relationshipWeightProperty: 'foo'" +
                       "}) YIELD writeMillis, writeProperty";
        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", NODES, "relQuery", OUTGOING_RELS),
            row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", outgoingWeightedExpected);
    }

}
