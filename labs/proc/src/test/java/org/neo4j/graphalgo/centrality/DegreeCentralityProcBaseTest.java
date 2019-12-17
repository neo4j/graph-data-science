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

package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.ProcTestBase;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

class DegreeCentralityProcBaseTest extends ProcTestBase {

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
        registerProcedures(DegreeCentralityProcBase.class);

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

    @Test
    public void testDegreeIncomingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(INCOMING)
            .streamMode()
            .addParameter("direction", INCOMING)
            .yields("nodeId", "score");
        runQuery(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(incomingExpected, actual);
    }

    @Test
    public void testWeightedDegreeIncomingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(INCOMING)
            .streamMode()
            .addParameter("weightProperty", "foo")
            .addParameter("direction", INCOMING)
            .yields("nodeId", "score");
        runQuery(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(incomingWeightedExpected, actual);
    }

    @Test
    public void testDegreeIncomingWriteBack() {
        String query = queryBuilder(INCOMING)
            .writeMode()
            .addParameter("direction", INCOMING)
            .yields("writeMillis", "write", "writeProperty");
        runQuery(query, row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(
                    row.getNumber("writeMillis").intValue() >= 0,
                    "write time not set");
            }
        );
        assertResult("degree", incomingExpected);
    }

    @Test
    public void testWeightedDegreeIncomingWriteBack() {
        String query = queryBuilder(INCOMING)
            .writeMode()
            .addParameter("direction", INCOMING)
            .addParameter("weightProperty", "foo")
            .yields("writeMillis", "write", "writeProperty");
        runQuery(query, row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(
                    row.getNumber("writeMillis").intValue() >= 0,
                    "write time not set");
            }
        );
        assertResult("degree", incomingWeightedExpected);
    }

    @Test
    public void testDegreeBothStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(BOTH)
            .streamMode()
            .addParameter("direction", BOTH)
            .yields("nodeId", "score");
        runQuery(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(bothExpected, actual);
    }

    @Test
    public void testWeightedDegreeBothStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(BOTH)
            .streamMode()
            .addParameter("direction", BOTH)
            .addParameter("weightProperty", "foo")
            .yields("nodeId", "score");
        runQuery(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(bothWeightedExpected, actual);
    }

    @Test
    public void testDegreeBothWriteBack() {
        String query = queryBuilder(BOTH)
            .writeMode()
            .addParameter("direction", BOTH)
            .yields("writeMillis", "write", "writeProperty");
        runQuery(query, row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", bothExpected);
    }

    @Test
    public void testWeightedDegreeBothWriteBack() {
        String query = queryBuilder(BOTH)
            .writeMode()
            .addParameter("direction", BOTH)
            .addParameter("weightProperty", "foo")
            .yields("writeMillis", "write", "writeProperty");
        runQuery(query, row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", bothWeightedExpected);
    }

    @Test
    public void testDegreeOutgoingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(OUTGOING)
            .streamMode()
            .addParameter("direction", OUTGOING)
            .yields("nodeId", "score");
        runQuery(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score")));
        assertMapEquals(outgoingExpected, actual);
    }

    @Test
    public void testWeightedDegreeOutgoingStream() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder(OUTGOING)
            .streamMode()
            .addParameter("direction", OUTGOING)
            .addParameter("weightProperty", "foo")
            .yields("nodeId", "score");
        runQuery(query, row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(outgoingWeightedExpected, actual);
    }

    @Test
    public void testDegreeOutgoingWriteBack() {
        String query = queryBuilder(OUTGOING)
            .writeMode()
            .addParameter("direction", OUTGOING)
            .yields("writeMillis", "write", "writeProperty");
        runQuery(query, row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", outgoingExpected);
    }

    @Test
    public void testWeightedDegreeOutgoingWriteBack() {
        String query = queryBuilder(OUTGOING)
            .writeMode()
            .addParameter("direction", OUTGOING)
            .addParameter("weightProperty", "foo")
            .yields("writeMillis", "write", "writeProperty");
        runQuery(query, row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("degree", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("degree", outgoingWeightedExpected);
    }

    private GdsCypher.ModeBuildStage queryBuilder(Direction direction) {
        return GdsCypher
            .call()
            .withNodeLabel("Label1")
            .withRelationshipType(
                "TYPE1",
                RelationshipProjection.builder().type("TYPE1").projection(projection(direction)).build()
            )
            .withRelationshipProperty("foo")
            .algo("gds.alpha.degree");
    }

    private Projection projection(Direction direction) {
        switch (direction) {
            case OUTGOING:
                return Projection.NATURAL;
            case INCOMING:
                return Projection.REVERSE;
            case BOTH:
                return Projection.UNDIRECTED;
            default:
                throw new IllegalArgumentException("Unexpected value: " + direction + " (sad java 😞)");
        }
    }

}
