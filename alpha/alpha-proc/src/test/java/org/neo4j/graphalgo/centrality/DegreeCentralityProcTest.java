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
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.Orientation.NATURAL;
import static org.neo4j.graphalgo.Orientation.REVERSE;
import static org.neo4j.graphalgo.Orientation.UNDIRECTED;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.findNode;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.runInTransaction;

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

        runInTransaction(db, tx -> {
            Label label = Label.label("Label1");
            incomingExpected.put(findNode(db, tx, label, "name", "a").getId(), 0.0);
            incomingExpected.put(findNode(db, tx, label, "name", "b").getId(), 1.0);
            incomingExpected.put(findNode(db, tx, label, "name", "c").getId(), 2.0);

            incomingWeightedExpected.put(findNode(db, tx, label, "name", "a").getId(), 0.0);
            incomingWeightedExpected.put(findNode(db, tx, label, "name", "b").getId(), 3.0);
            incomingWeightedExpected.put(findNode(db, tx, label, "name", "c").getId(), 7.1);

            bothExpected.put(findNode(db, tx, label, "name", "a").getId(), 2.0);
            bothExpected.put(findNode(db, tx, label, "name", "b").getId(), 2.0);
            bothExpected.put(findNode(db, tx, label, "name", "c").getId(), 2.0);

            bothWeightedExpected.put(findNode(db, tx, label, "name", "a").getId(), 5.1);
            bothWeightedExpected.put(findNode(db, tx, label, "name", "b").getId(), 8.0);
            bothWeightedExpected.put(findNode(db, tx, label, "name", "c").getId(), 7.1);

            outgoingExpected.put(findNode(db, tx, label, "name", "a").getId(), 2.0);
            outgoingExpected.put(findNode(db, tx, label, "name", "b").getId(), 1.0);
            outgoingExpected.put(findNode(db, tx, label, "name", "c").getId(), 0.0);

            outgoingWeightedExpected.put(findNode(db, tx, label, "name", "a").getId(), 5.1);
            outgoingWeightedExpected.put(findNode(db, tx, label, "name", "b").getId(), 5.0);
            outgoingWeightedExpected.put(findNode(db, tx, label, "name", "c").getId(), 0.0);
        });
    }

    @Test
    public void testDegreeIncomingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(REVERSE)
            .streamMode()
            .yields("nodeId", "score");
        runQueryWithRowConsumer(
            query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(incomingExpected, actual);
    }

    @Test
    public void testWeightedDegreeIncomingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(REVERSE)
            .streamMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields("nodeId", "score");
        runQueryWithRowConsumer(
            query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(incomingWeightedExpected, actual);
    }

    @Test
    public void testDegreeIncomingWriteBack() {
        String query = queryBuilder(REVERSE)
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> {
                assertNotEquals(-1L, row.getNumber("createMillis").longValue());
                assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
                assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(
                    row.getNumber("writeMillis").intValue() >= 0,
                    "write time not set"
                );
            }
        );
        assertResult("degree", incomingExpected);
    }

    @Test
    public void testWeightedDegreeIncomingWriteBack() {
        String query = queryBuilder(REVERSE)
            .writeMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields("writeMillis", "writeProperty");
        runQueryWithRowConsumer(query, row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(
                    row.getNumber("writeMillis").intValue() >= 0,
                    "write time not set"
                );
            }
        );
        assertResult("degree", incomingWeightedExpected);
    }

    @Test
    public void testDegreeBothStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(UNDIRECTED)
            .streamMode()
            .yields("nodeId", "score");
        runQueryWithRowConsumer(
            query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(bothExpected, actual);
    }

    @Test
    public void testWeightedDegreeBothStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(UNDIRECTED)
            .streamMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields("nodeId", "score");
        runQueryWithRowConsumer(
            query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(bothWeightedExpected, actual);
    }

    @Test
    public void testDegreeBothWriteBack() {
        String query = queryBuilder(UNDIRECTED)
            .writeMode()
            .yields("writeMillis", "writeProperty");
        runQueryWithRowConsumer(query, row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", bothExpected);
    }

    @Test
    public void testWeightedDegreeBothWriteBack() {
        String query = queryBuilder(UNDIRECTED)
            .writeMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields("writeMillis", "writeProperty");
        runQueryWithRowConsumer(query, row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", bothWeightedExpected);
    }

    @Test
    public void testDegreeOutgoingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(NATURAL)
            .streamMode()
            .yields("nodeId", "score");
        runQueryWithRowConsumer(
            query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingExpected, actual);
    }

    @Test
    public void testWeightedDegreeOutgoingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(NATURAL)
            .streamMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields("nodeId", "score");
        runQueryWithRowConsumer(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingWeightedExpected, actual);
    }

    @Test
    public void testDegreeOutgoingWriteBack() {
        String query = queryBuilder(NATURAL)
            .writeMode()
            .yields("writeMillis", "writeProperty");
        runQueryWithRowConsumer(query, row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", outgoingExpected);
    }

    @Test
    public void testWeightedDegreeOutgoingWriteBack() {
        String query = queryBuilder(NATURAL)
            .writeMode()
            .addParameter("relationshipWeightProperty", "foo")
            .yields("writeMillis", "writeProperty");
        runQueryWithRowConsumer(query, row -> {
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", outgoingWeightedExpected);
    }

    private GdsCypher.ModeBuildStage queryBuilder(Orientation orientation) {
        return GdsCypher
            .call()
            .withNodeLabel("Label1")
            .withRelationshipType(
                "TYPE1",
                RelationshipProjection.builder().type("TYPE1").orientation(orientation).build()
            )
            .withRelationshipProperty("foo")
            .algo("gds.alpha.degree");
    }

}
