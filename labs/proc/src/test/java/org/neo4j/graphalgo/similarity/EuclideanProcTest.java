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
package org.neo4j.graphalgo.similarity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.Map;

import static java.lang.Math.sqrt;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;

class EuclideanProcTest extends BaseProcTest {

    private static final String STATEMENT_STREAM =
        " MATCH (i:Item)" +
        " WITH i ORDER BY id(i)" +
        " MATCH (p:Person)" +
        " OPTIONAL MATCH (p)-[r:LIKES]->(i)" +
        " WITH {item:id(p), weights: collect(coalesce(r.stars, $missingValue))} AS userData" +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.euclidean.stream(input)" +
        " YIELD item1, item2, count1, count2, intersection, similarity " +
        " RETURN item1, item2, count1, count2, intersection, similarity " +
        " ORDER BY item1, item2";

    private static final String STATEMENT_CYPHER_STREAM =
        " CALL gds.alpha.similarity.euclidean.stream($config) " +
        " YIELD item1, item2, count1, count2, intersection, similarity " +
        " RETURN * " +
        " ORDER BY item1, item2";

    private static final String STATEMENT =
        " MATCH (i:Item)" +
        " WITH i ORDER BY id(i)" +
        " MATCH (p:Person)" +
        " OPTIONAL MATCH (p)-[r:LIKES]->(i)" +
        " WITH {item:id(p), weights: collect(coalesce(r.stars, 0))} AS userData" +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.euclidean.write(input)" +
        " YIELD p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations" +
        " RETURN *";

    private static final String STORE_EMBEDDING_STATEMENT =
        " MATCH (i:Item)" +
        " WITH i ORDER BY id(i)" +
        " MATCH (p:Person)" +
        " OPTIONAL MATCH (p)-[r:LIKES]->(i)" +
        " WITH p, collect(coalesce(r.stars, 0)) AS userData" +
        " SET p.embedding = userData";

    private static final String EMBEDDING_STATEMENT =
        " MATCH (p:Person)" +
        " WITH {item:id(p), weights: p.embedding} AS userData" +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.euclidean.write(input)" +
        " YIELD p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs" +
        " RETURN *";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(EuclideanProc.class);
        runQuery(buildDatabaseQuery());
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    private void buildRandomDB(int size) {
        runQuery("MATCH (n) DETACH DELETE n");
        runQuery("UNWIND range(1,$size/10) AS _ CREATE (:Person) CREATE (:Item) ", singletonMap("size", size));
        String statement =
            "MATCH (p:Person) WITH collect(p) AS people " +
            "MATCH (i:Item) WITH people, collect(i) AS items " +
            "UNWIND range(1,$size) AS _ " +
            "WITH people[toInteger(rand()*size(people))] AS p, items[toInteger(rand()*size(items))] AS i " +
            "MERGE (p)-[:LIKES]->(i) RETURN COUNT(*) ";
        runQuery(statement, singletonMap("size", size));
    }

    private String buildDatabaseQuery() {
        return "CREATE (a:Person {name:'Alice'})\n" +
               "CREATE (b:Person {name:'Bob'})\n" +
               "CREATE (c:Person {name:'Charlie'})\n" +
               "CREATE (d:Person {name:'Dana'})\n" +
               "CREATE (i1:Item {name:'p1'})\n" +
               "CREATE (i2:Item {name:'p2'})\n" +
               "CREATE (i3:Item {name:'p3'})\n" +

               "CREATE" +
               " (a)-[:LIKES {stars:1}]->(i1),\n" +
               " (a)-[:LIKES {stars:2}]->(i2),\n" +
               " (a)-[:LIKES {stars:5}]->(i3),\n" +

               " (b)-[:LIKES {stars:1}]->(i1),\n" +
               " (b)-[:LIKES {stars:3}]->(i2),\n" +

               " (c)-[:LIKES {stars:4}]->(i3)\n";
        // a: 1,2,5
        // b: 1,3,0
        // c: 0,0,4
        // a - b: sqrt(26) = 5.1
        // a - c: sqrt(6) = 2.5
        // b - c: sqrt(26) = 5.1
    }

    @Test
    void euclideanSingleMultiThreadComparision() {
        int size = 333;
        buildRandomDB(size);
        try(
            Result result1 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "concurrency", 1, "topK", 0), "missingValue", 0));
            Result result2 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "concurrency", 2, "topK", 0), "missingValue", 0));
            Result result4 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "concurrency", 4, "topK", 0), "missingValue", 0));
            Result result8 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "concurrency", 8, "topK", 0), "missingValue", 0))
        ) {
            int count = 0;
            while (result1.hasNext()) {
                Map<String, Object> row1 = result1.next();
                assertEquals(row1, result2.next(), row1.toString());
                assertEquals(row1, result4.next(), row1.toString());
                assertEquals(row1, result8.next(), row1.toString());
                count++;
            }
            int people = size / 10;
            assertEquals((people * people - people) / 2, count);
        }
    }

    @Test
    void euclideanSingleMultiThreadComparisionTopK() {
        int size = 333;
        buildRandomDB(size);

        try(
            Result result1 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "topK", 1, "concurrency", 1), "missingValue", 0));
            Result result2 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "topK", 1, "concurrency", 2), "missingValue", 0));
            Result result4 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "topK", 1, "concurrency", 4), "missingValue", 0));
            Result result8 = runQueryWithoutClosing(STATEMENT_STREAM, map("config", map("similarityCutoff", -0.1, "topK", 1, "concurrency", 8), "missingValue", 0))
        ) {
            int count = 0;
            while (result1.hasNext()) {
                Map<String, Object> row1 = result1.next();
                assertEquals(row1, result2.next(), row1.toString());
                assertEquals(row1, result4.next(), row1.toString());
                assertEquals(row1, result8.next(), row1.toString());
                count++;
            }
            int people = size / 10;
            assertEquals(people, count);
        }
    }

    @Test
    void topNeuclideanStreamTest() {
        runQuery(STATEMENT_STREAM, map("config", map("top", 2, "topK", 0), "missingValue", 0),
            results -> {
                assert02(results.next());
                assert13(results.next());
                assertFalse(results.hasNext());
            }
        );
    }

    @Test
    void euclideanStreamTest() {
        // System.out.println(runQuery("MATCH (i:Item) WITH i ORDER BY id(i) MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(i) RETURN p,r,i").resultAsString());
        // a: 1,2,5
        // b: 1,3,0
        // c: 0,0,4
        // d: 0,0,0
        // a0 - b1: sqrt(26) = 5.1
        // a0 - c2: sqrt(6) = 2.5
        // a0 - d3: sqrt(1+4+25) = 5.5
        // b1 - c2: sqrt(26) = 5.1
        // b1 - d3: sqrt(10) =  3.2
        // c2 - d3: sqrt(16) = 4
        // System.out.println(runQuery(query).resultAsString());

        runQuery(STATEMENT_STREAM, map("config", map("concurrency", 1, "topK", 0), "missingValue", 0),
            results -> {
                assertTrue(results.hasNext());
                assert01(results.next());
                assert02(results.next());
                assert03(results.next());
                assert12(results.next());
                assert13(results.next());
                assert23(results.next());
                assertFalse(results.hasNext());
            }
        );
    }

    @Test
    void euclideanSourceTargetIdsStreamTest() {
        Map<String, Object> config = map(
            "concurrency", 1,
            "sourceIds", Collections.singletonList(0L),
            "targetIds", Collections.singletonList(1L)
        );
        runQuery(STATEMENT_STREAM, map("config", config, "missingValue", 0),
            results -> {
                assertTrue(results.hasNext());
                assert01(results.next());
                assertFalse(results.hasNext());
            }
        );
    }

    @Test
    void euclideanCypherStreamTest() {
        // System.out.println(runQuery("MATCH (i:Item) WITH i ORDER BY id(i) MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(i) RETURN p,r,i").resultAsString());
        // a: 1,2,5
        // b: 1,3,0
        // c: 0,0,4
        // d: 0,0,0
        // a0 - b1: sqrt(26) = 5.1
        // a0 - c2: sqrt(6) = 2.5
        // a0 - d3: sqrt(1+4+25) = 5.5
        // b1 - c2: sqrt(26) = 5.1
        // b1 - d3: sqrt(10) =  3.2
        // c2 - d3: sqrt(16) = 4
        // System.out.println(runQuery(query).resultAsString());

        String query = "MATCH (i:Item), (p:Person)" +
                       "OPTIONAL MATCH (p)-[r:LIKES]->(i) " +
                       "RETURN id(p) AS item, id(i) AS category, coalesce(r.stars, 0) AS weight";

        Map<String, Object> params = map(
            "config", map("concurrency", 1, "graph", "cypher", "skipValue", Double.NaN, "data", query, "topK", 0)
        );
        runQuery(STATEMENT_CYPHER_STREAM, params,
            results -> {
                assertTrue(results.hasNext());
                assert01(results.next());
                assert02(results.next());
                assert03(results.next());
                assert12(results.next());
                assert13(results.next());
                assert23(results.next());
                assertFalse(results.hasNext());
            }
        );
    }

    @Test
    void eucideanSkipStreamTest() {
        runQuery(
            STATEMENT_STREAM,
            map("config", map("concurrency", 1, "skipValue", Double.NaN, "topK", 0), "missingValue", Double.NaN),
            results -> {
                assertTrue(results.hasNext());
                assert01Skip(results.next());
                assert02Skip(results.next());
                assert12Skip(results.next());
                assertFalse(results.hasNext());
            }
        );
    }

    @Test
    void topKEuclideanStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 1, "topK", 1), "missingValue", 0);

        runQuery(STATEMENT_STREAM, params,
            results -> {
                assertTrue(results.hasNext());
                assert02(results.next());
                assert13(results.next());
                assert02(flip(results.next()));
                assert13(flip(results.next()));
                assertFalse(results.hasNext());
            }
        );
    }

    @Test
    void topKEuclideanSourceTargetIdStreamTest() {
        Map<String, Object> config = map(
            "concurrency", 1,
            "topK", 1,
            "sourceIds", Collections.singletonList(0L)

        );
        Map<String, Object> params = map("config", config, "missingValue", 0);

        runQuery(STATEMENT_STREAM, params, results -> {
            assertTrue(results.hasNext());
            assert02(results.next());
            assertFalse(results.hasNext());
        });
    }

    private Map<String, Object> flip(Map<String, Object> row) {
        return map("similarity", row.get("similarity"), "intersection", row.get("intersection"),
            "item1", row.get("item2"), "count1", row.get("count2"),
            "item2", row.get("item1"), "count2", row.get("count1")
        );
    }

    private void assertSameSource(Result results, int count, long source) {
        Map<String, Object> row;
        long target = 0;
        for (int i = 0; i < count; i++) {
            if (target == source) target++;
            assertTrue(results.hasNext());
            row = results.next();
            assertEquals(source, row.get("item1"));
            assertEquals(target, row.get("item2"));
            target++;
        }
    }

    @Test
    void topK4euclideanStreamTest() {
        Map<String, Object> params = map(
            "config",
            map("topK", 4, "concurrency", 4, "similarityCutoff", -0.1),
            "missingValue",
            0
        );
        runQuery(STATEMENT_STREAM, params, results -> {
            assertSameSource(results, 3, 0L);
            assertSameSource(results, 3, 1L);
            assertSameSource(results, 3, 2L);
            assertSameSource(results, 3, 3L);
            assertFalse(results.hasNext());
        });
    }

    @Test
    void topK3euclideanStreamTest() {
        // a0 - b1: sqrt(26) = 5.1
        // a0 - c2: sqrt(6) = 2.5
        // a0 - d3: sqrt(1+4+25) = 5.5
        // b1 - c2: sqrt(26) = 5.1
        // b1 - d3: sqrt(10) =  3.2
        // c2 - d3: sqrt(16) = 4
        Map<String, Object> params = map("config", map("concurrency", 3, "topK", 3), "missingValue", 0);

        runQuery(STATEMENT_STREAM, params, results -> {
            assertSameSource(results, 3, 0L);
            assertSameSource(results, 3, 1L);
            assertSameSource(results, 3, 2L);
            assertSameSource(results, 3, 3L);
            assertFalse(results.hasNext());
        });
    }

    @Test
    void simpleEuclideanTest() {
        Map<String, Object> params = map("config", map(), "missingValue", 0);

        Map<String, Object> row = runQuery(STATEMENT, params, Result::next);
        assertEquals((double) row.get("p25"), 3.16, 0.01);
        assertEquals((double) row.get("p50"), 4.00, 0.01);
        assertEquals((double) row.get("p75"), 5.10, 0.01);
        assertEquals((double) row.get("p95"), 5.48, 0.01);
        assertEquals((double) row.get("p99"), 5.48, 0.01);
        assertEquals((double) row.get("p100"), 5.48, 0.01);
    }

    @Test
    void simpleEuclideanFromEmbeddingTest() {
        runQuery(STORE_EMBEDDING_STATEMENT);

        Map<String, Object> params = map("config", map());

        Map<String, Object> row = runQuery(EMBEDDING_STATEMENT, params, Result::next);
        assertEquals((double) row.get("p25"), 3.16, 0.01);
        assertEquals((double) row.get("p50"), 4.00, 0.01);
        assertEquals((double) row.get("p75"), 5.10, 0.01);
        assertEquals((double) row.get("p95"), 5.48, 0.01);
        assertEquals((double) row.get("p99"), 5.48, 0.01);
        assertEquals((double) row.get("p100"), 5.48, 0.01);
    }

    @Test
    void simpleEuclideanWriteTest() {
        Map<String, Object> params = map(
            "config",
            map("write", true, "similarityCutoff", 4.0, "concurrency", 1, "topK", 0),
            "missingValue",
            0
        );

        runQuery(STATEMENT, params);

        String checkSimilaritiesQuery = "MATCH (a)-[similar:SIMILAR]-(b)" +
                                        "RETURN a.name AS node1, b.name AS node2, similar.score AS score " +
                                        "ORDER BY id(a), id(b)";

        runQuery(checkSimilaritiesQuery, result -> {
            // a0 - b1: sqrt(26) = 5.1
            // a0 - c2: sqrt(6) = 2.5
            // a0 - d3: sqrt(1+4+25) = 5.5
            // b1 - c2: sqrt(26) = 5.1
            // b1 - d3: sqrt(10) =  3.2
            // c2 - d3: sqrt(16) = 4

            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals(row.get("node1"), "Alice");
            assertEquals(row.get("node2"), "Charlie");
            assertEquals((double) row.get("score"), 2.45, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Bob");
            assertEquals(row.get("node2"), "Dana");
            assertEquals((double) row.get("score"), 3.16, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Charlie");
            assertEquals(row.get("node2"), "Alice");
            assertEquals((double) row.get("score"), 2.45, 0.01);


            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Charlie");
            assertEquals(row.get("node2"), "Dana");
            assertEquals((double) row.get("score"), 4.0, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Dana");
            assertEquals(row.get("node2"), "Bob");
            assertEquals((double) row.get("score"), 3.16, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Dana");
            assertEquals(row.get("node2"), "Charlie");
            assertEquals((double) row.get("score"), 4.0, 0.01);

            assertFalse(result.hasNext());
        });
    }

    @Test
    void dontComputeComputationsByDefault() {
        Map<String, Object> params = map("config", map(
            "write", true,
            "similarityCutoff", 0.1
        ));

        Map<String, Object> writeRow = runQuery(STATEMENT, params, Result::next);
        assertEquals(-1L, (long) writeRow.get("computations"));
    }

    @Test
    void numberOfComputations() {
        Map<String, Object> params = map("config", map(
            "write", true,
            "showComputations", true,
            "similarityCutoff", 0.1
        ));

        Map<String, Object> writeRow = runQuery(STATEMENT, params, Result::next);
        assertEquals(6L, (long) writeRow.get("computations"));
    }

    private void assert23(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(sqrt(16), row.get("similarity"));
    }

    private void assert23Skip(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(1L, row.get("count1"));
        assertEquals(0L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.0, row.get("similarity"));
    }

    private void assert13(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(sqrt(10), row.get("similarity"));
    }

    private void assert13Skip(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(0L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.0, row.get("similarity"));
    }

    private void assert12(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(sqrt(5 * 5 + 1), row.get("similarity"));
    }

    private void assert12Skip(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(Double.NaN, row.get("similarity"));
    }

    private void assert03(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(sqrt(5 * 5 + 2 * 2 + 1), row.get("similarity"));
    }

    private void assert03Skip(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(0L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.0, row.get("similarity"));
    }

    private void assert02(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        // assertEquals(1L, row.get("intersection"));
        assertEquals(sqrt(6), row.get("similarity"));
    }

    private void assert02Skip(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        // assertEquals(1L, row.get("intersection"));
        assertEquals(sqrt(1), row.get("similarity"));
    }

    private void assert01(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(sqrt(5 * 5 + 1), row.get("similarity"));
    }

    private void assert01Skip(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(sqrt(1), row.get("similarity"));
    }
}
