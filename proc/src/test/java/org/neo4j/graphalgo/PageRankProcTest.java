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
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.core.loading.UserGraphCatalog;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageRankProcTest extends ProcTestBase {

    private static Map<Long, Double> expected = new HashMap<>();
    private static Map<Long, Double> weightedExpected = new HashMap<>();

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
            ", (u:Label3 {name: 'u'})" +
            ", (v:Label3 {name: 'v'})" +
            ", (w:Label3 {name: 'w'})" +
            ", (b)-[:TYPE1 {weight: 1.0,  equalWeight: 1.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 1.2,  equalWeight: 1.0}]->(b)" +
            ", (d)-[:TYPE1 {weight: 1.3,  equalWeight: 1.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 1.7,  equalWeight: 1.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 6.1,  equalWeight: 1.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.2,  equalWeight: 1.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 1.5,  equalWeight: 1.0}]->(f)" +
            ", (f)-[:TYPE1 {weight: 10.5, equalWeight: 1.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.9,  equalWeight: 1.0}]->(e)" +
            ", (g)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(b)" +
            ", (g)-[:TYPE2 {weight: 5.3,  equalWeight: 1.0}]->(e)" +
            ", (h)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(b)" +
            ", (h)-[:TYPE2 {weight: 0.3,  equalWeight: 1.0}]->(e)" +
            ", (i)-[:TYPE2 {weight: 5.4,  equalWeight: 1.0}]->(b)" +
            ", (i)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(e)" +
            ", (j)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(e)" +
            ", (k)-[:TYPE2 {weight: 4.2,  equalWeight: 1.0}]->(e)" +
            ", (u)-[:TYPE3 {weight: 1.0}]->(v)" +
            ", (u)-[:TYPE3 {weight: 1.0}]->(w)" +
            ", (v)-[:TYPE3 {weight: 1.0}]->(w)";

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(DB_CYPHER).close();
            tx.success();
        }

        registerProcedures(GraphLoadProc.class, PageRankProc.class);

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

    @AllGraphNamesTest
    void testPageRankStream(String graphImpl) {
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

    @AllGraphNamesTest
    void testPageRankStreamFromLoadedGraph(String graphImpl) {
        String graphName = "aggGraph";
        UserGraphCatalog.remove(getUsername(), graphName);

        String loadQuery = String.format(
                "CALL algo.graph.load(" +
                "    '%s', 'Label1', 'TYPE1', {" +
                "        graph: $graph" +
                "    }" +
                ")", graphName);

        db.execute(loadQuery, MapUtil.map("graph", graphImpl));

        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    '', '', {" +
                       "         graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphName),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @AllGraphNamesTest
    void testWeightedPageRankStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @AllGraphNamesTest
    void testWeightedPageRankStreamFromLoadedGraph(String graphImpl) {
        String graphName = "aggGraph";
        UserGraphCatalog.remove(getUsername(), graphName);

        String loadQuery = String.format(
                "CALL algo.graph.load(" +
                "    '%s', 'Label1', 'TYPE1', {" +
                "        graph: $graph, relationshipWeight: 'weight'" +
                "    }" +
                ")", graphName);

        db.execute(loadQuery, MapUtil.map("graph", graphImpl));

        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    '', '', {" +
                       "        graph: $graph, weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphName),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @AllGraphNamesTest
    void testWeightedPageRankStreamFromLoadedGraphWithDirectionBoth(String graphImpl) {
        String graphName = "fooGraph";
        String loadQuery = String.format(
                "CALL algo.graph.load(" +
                "    '%s', 'Label3', 'TYPE3', {" +
                "        graph: $graph, relationshipWeight: 'equalWeight', direction: 'BOTH'" +
                "    }" +
                ")", graphName);

        db.execute(loadQuery, MapUtil.map("graph", graphImpl));

        String query = "CALL algo.pageRank.stream(" +
                       "    '', '', {" +
                       "        graph: $graph, " +
                       "        weightProperty: 'equalWeight', " +
                       "        direction: 'BOTH'," +
                       "        iterations: 1" +
                       "    }" +
                       ") YIELD nodeId, score";

        final Map<Long, Double> actual = new HashMap<>();
        runQuery(query, MapUtil.map("graph", graphName),
                row -> actual.put(row.getNumber("nodeId").longValue(), row.getNumber("score").doubleValue()));

        long distinctValues = actual.values().stream().distinct().count();
        assertEquals(1, distinctValues);
    }

    @AllGraphNamesTest
    void testWeightedPageRankStreamThrowsIfWeightPropertyDoesNotExist(String graphImpl) {
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'does_not_exist'" +
                       "    }" +
                       ") YIELD nodeId, score";
        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            runQuery(query, MapUtil.map("graph", graphImpl), row -> {});
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertEquals("Relationship properties not found: 'does_not_exist'", rootCause.getMessage());
    }

    @AllGraphNamesTest
    void testWeightedPageRankWithCachedWeightsStream(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.pageRank.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'weight', cacheWeights: true" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @AllGraphNamesTest
    void testWeightedPageRankWithAllRelationshipsEqualStream(String graphImpl) {
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


    @AllGraphNamesTest
    void testPageRankWriteBack(String graphImpl) {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("pagerank", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("pagerank", expected);
    }

    @AllGraphNamesTest
    void testWeightedPageRankWriteBack(String graphImpl) {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        graph: $graph, weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("pagerank", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("pagerank", weightedExpected);
    }

    @AllGraphNamesTest
    void testPageRankWriteBackUnderDifferentProperty(String graphImpl) {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        writeProperty: 'foobar', graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("foobar", row.getString("writeProperty"));
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        assertResult("foobar", expected);
    }

    @AllGraphNamesTest
    void testPageRankParallelWriteBack(String graphImpl) {
        String query = "CALL algo.pageRank(" +
                       "    'Label1', 'TYPE1', {" +
                       "        batchSize: 3, write: true, graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set")
        );
        assertResult("pagerank", expected);
    }

    @AllGraphNamesTest
    void testPageRankParallelExecution(String graphImpl) {
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

    @AllGraphNamesTest
    void testPageRankWithToleranceParam(String graphImpl) {
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
                row -> assertEquals(1L, (long) row.getNumber("iterations")));
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

}
