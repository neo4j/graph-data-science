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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphalgo.pagerank.PageRankWriteProc;
import org.neo4j.graphdb.Label;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

@Disabled(value = "This should be fixed to use `Start Node(s)` when calling the procedure, also should be changed to use the new procedure API")
class PersonalizedPageRankProcTest extends BaseProcTest {

    private static final Map<Long, Double> EXPECTED = new HashMap<>();

    @Language("Cypher")
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

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
        registerProcedures(
            PageRankWriteProc.class,
            PageRankStreamProc.class
        );

        runInTransaction(db, () -> {
            final Label label = Label.label("Label1");
            EXPECTED.put(db.findNode(label, "name", "a").getId(), 0.243);
            EXPECTED.put(db.findNode(label, "name", "b").getId(), 1.844);
            EXPECTED.put(db.findNode(label, "name", "c").getId(), 1.777);
            EXPECTED.put(db.findNode(label, "name", "d").getId(), 0.218);
            EXPECTED.put(db.findNode(label, "name", "e").getId(), 0.243);
            EXPECTED.put(db.findNode(label, "name", "f").getId(), 0.218);
            EXPECTED.put(db.findNode(label, "name", "g").getId(), 0.150);
            EXPECTED.put(db.findNode(label, "name", "h").getId(), 0.150);
            EXPECTED.put(db.findNode(label, "name", "i").getId(), 0.150);
            EXPECTED.put(db.findNode(label, "name", "j").getId(), 0.150);
        });
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphNamesTest
    void testPageRankStream(String graphName) {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
            "CALL gds.algo.pageRank.stream('Label1', 'TYPE1') YIELD nodeId, score",
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEquals(actual);
    }

    @AllGraphNamesTest
    void testPageRankWriteBack(String graphName) {
        runQuery(
            "CALL gds.algo.pageRank.write('Label1', 'TYPE1', {graph:'" + graphName + "'}) YIELD writeMillis, write, writeProperty",
            row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("pagerank", row.getString("writeProperty"));
                assertTrue(
                    row.getNumber("writeMillis").intValue() >= 0,
                    "write time not set"
                );
            }
        );

        assertResult("pagerank");
    }

    @AllGraphNamesTest
    void testPageRankWriteBackUnderDifferentProperty(String graphName) {
        runQuery(
            "CALL gds.algo.pageRank.write('Label1', 'TYPE1', {writeProperty:'foobar', graph:'" + graphName + "'}) YIELD writeMillis, write, writeProperty",
            row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("foobar", row.getString("writeProperty"));
                assertTrue(
                    row.getNumber("writeMillis").intValue() >= 0,
                    "write time not set"
                );
            }
        );

        assertResult("foobar");
    }

    @AllGraphNamesTest
    void testPageRankParallelWriteBack(String graphName) {
        runQuery(
            "CALL gds.algo.pageRank.write('Label1', 'TYPE1', {batchSize:3, write:true, graph:'" + graphName + "'}) YIELD writeMillis, write, writeProperty",
            row -> assertTrue(
                row.getNumber("writeMillis").intValue() >= 0,
                "write time not set"
            )
        );

        assertResult("pagerank");
    }

    @AllGraphNamesTest
    void testPageRankParallelExecution(String graphName) {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
            "CALL gds.algo.pageRank.stream('Label1', 'TYPE1', {batchSize:2, graph:'" + graphName + "'}) YIELD nodeId, score",
            row -> {
                final long nodeId = row.getNumber("nodeId").longValue();
                actual.put(nodeId, (Double) row.get("score"));
            }
        );
        assertMapEquals(actual);
    }

    private void assertResult(final String scoreProperty) {
        runInTransaction(db, () -> {
            for (Map.Entry<Long, Double> entry : EXPECTED.entrySet()) {
                double score = ((Number) db
                    .getNodeById(entry.getKey())
                    .getProperty(scoreProperty)).doubleValue();
                assertEquals(
                    entry.getValue(),
                    score,
                    0.1,
                    "score for " + entry.getKey()
                );
            }
        });
    }

    private static void assertMapEquals(Map<Long, Double> actual) {
        assertEquals(EXPECTED.size(), actual.size(), "number of elements");
        Set<Long> expectedKeys = new HashSet<>(EXPECTED.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                expectedKeys.remove(entry.getKey()),
                "unknown key " + entry.getKey()
            );
            assertEquals(
                EXPECTED.get(entry.getKey()),
                entry.getValue(),
                0.1,
                "value for " + entry.getKey()
            );
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }
}
