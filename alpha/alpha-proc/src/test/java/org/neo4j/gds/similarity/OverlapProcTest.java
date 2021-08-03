/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.similarity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.impl.similarity.CategoricalInput;
import org.neo4j.gds.impl.similarity.OverlapAlgorithm;
import org.neo4j.gds.impl.similarity.SimilarityConfig;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.compat.MapUtil.map;

class OverlapProcTest extends AlphaSimilarityProcTest<OverlapAlgorithm, CategoricalInput> {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Person {name: 'Alice'})" +
                                            " ,(b:Person {name: 'Bob'})" +
                                            " ,(c:Person {name: 'Charlie'})" +
                                            " ,(d:Person {name: 'Dana'})" +
                                            " ,(i1:Item  {name: 'p1'})" +
                                            " ,(i2:Item  {name: 'p2'})" +
                                            " ,(i3:Item  {name: 'p3'})" +
                                            " ,(a)-[:LIKES]->(i1)" +
                                            " ,(a)-[:LIKES]->(i2)" +
                                            " ,(a)-[:LIKES]->(i3)" +
                                            " ,(b)-[:LIKES]->(i1)" +
                                            " ,(b)-[:LIKES]->(i2)" +
                                            " ,(c)-[:LIKES]->(i3)";

    private static final String STATEMENT_STATS =
        " MATCH (p:Person)-[:LIKES]->(i:Item)" +
        " WITH {item: id(p), categories: collect(distinct id(i))} AS userData" +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.overlap.stats(input)" +
        " YIELD p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations" +
        " RETURN p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations";

    private static final String STATEMENT_STREAM =
        " MATCH (p:Person)-[:LIKES]->(i:Item)" +
        " WITH {item: id(p), categories: collect(distinct id(i))} AS userData" +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.overlap.stream(input)" +
        " YIELD item1, item2, count1, count2, intersection, similarity" +
        " RETURN item1, item2, count1, count2, intersection, similarity" +
        " ORDER BY item1, item2";

    private static final String STATEMENT =
        " MATCH (p:Person)-[:LIKES]->(i:Item)" +
        " WITH {item: id(p), categories: collect(distinct id(i))} AS userData" +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.overlap.write(input)" +
        " YIELD p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations" +
        " RETURN p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations";

    private static final String STORE_EMBEDDING_STATEMENT =
        " MATCH (p:Person)-[:LIKES]->(i:Item)" +
        " WITH p, collect(distinct id(i)) AS userData" +
        " SET p.embedding = userData";

    private static final String EMBEDDING_STATEMENT =
        " MATCH (p:Person)" +
        " WITH {item: id(p), categories: p.embedding} AS userData " +
        " WITH collect(userData) AS data, $config AS config" +
        " WITH config {.*, data: data} AS input" +
        " CALL gds.alpha.similarity.overlap.write(input)" +
        " YIELD p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs" +
        " RETURN p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(OverlapProc.class);
        runQuery(DB_CYPHER);
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
    }

    private void buildRandomDB(int size) {
        clearDb();
        runQuery("UNWIND range(1, $size / 10) AS _ CREATE (:Person), (:Item) ", singletonMap("size", size));
        String statement =
            "MATCH (p:Person) " +
            "WITH collect(p) AS people " +
            "MATCH (i:Item) " +
            "WITH people, collect(i) AS items " +
            "UNWIND range(1,$size) AS _ " +
            "WITH people[toInteger(rand()*size(people))] AS p, items[toInteger(rand()*size(items))] AS i " +
            "MERGE (p)-[:LIKES]->(i) RETURN count(*) ";
        runQuery(statement, singletonMap("size", size));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void overlapSingleMultiThreadComparison(int topK) {
        int size = 333;
        buildRandomDB(size);

        List<Map<String, Object>> expected = new ArrayList<>();

        runQueryWithResultConsumer(STATEMENT_STREAM,
            map("config", map("similarityCutoff", -0.1, "concurrency", 1, "topK", topK)), result -> {
                result.stream().forEach(expected::add);
            }
        );

        var resultKeys = List.of("item1", "item2", "count1", "count2", "intersection", "similarity");
        var expectedIterator = expected.iterator();

        runQueryWithRowConsumer(
            STATEMENT_STREAM,
            map("config", map("similarityCutoff", -0.1, "concurrency", 4, "topK", topK)),
            actualRow -> {
                Map<String, Object> expectedRow = expectedIterator.next();
                resultKeys.forEach(key -> assertThat(actualRow.get(key)).isEqualTo(expectedRow.get(key)));
            }
        );

        assertThat(expectedIterator.hasNext()).isFalse();
    }

    @Test
    void topNoverlapStreamTest() {
        runQueryWithResultConsumer(STATEMENT_STREAM, map("config", map("top", 2)), results -> {
            assert10(results.next());
            assert20(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    void overlapStreamTest() {
        runQueryWithResultConsumer(STATEMENT_STREAM, map("config", map("concurrency", 1)), results -> {
            assertTrue(results.hasNext());
            assert10(results.next());
            assert20(results.next());
            assert12(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    void overlapStreamSourceTargetIdsTest() {
        Map<String, Object> config = map(
            "concurrency", 1,
            "sourceIds", Collections.singletonList(1L)
        );

        Map<String, Object> params = map("config", config);

        runQueryWithResultConsumer(STATEMENT_STREAM, params, results -> {
            assertTrue(results.hasNext());
            assert10(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    void topKoverlapStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 1, "topK", 1));

        runQueryWithResultConsumer(STATEMENT_STREAM, params, results -> {
            assertTrue(results.hasNext());
            assert10(results.next());
            assert20(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    void topKoverlapSourceTargetIdsStreamTest() {
        Map<String, Object> config = map(
            "concurrency", 1,
            "topK", 1,
            "sourceIds", Collections.singletonList(1L)
        );
        Map<String, Object> params = map("config", config);

        runQueryWithResultConsumer(STATEMENT_STREAM, params, results -> {
            assertTrue(results.hasNext());
            assert10(results.next());
            assertFalse(results.hasNext());
        });
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
    void topK4overlapStreamTest() {
        Map<String, Object> params = map("config", map("topK", 4, "concurrency", 4, "similarityCutoff", -0.1));

        runQueryWithResultConsumer(STATEMENT_STREAM, params, results -> {
            assertSameSource(results, 0, 0L);
            assertSameSource(results, 1, 1L);
            assertSameSource(results, 2, 2L);
            assertFalse(results.hasNext());
        });
    }

    @Test
    void topK3overlapStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 3, "topK", 3));

        runQueryWithResultConsumer(STATEMENT_STREAM, params, results -> {
            assertSameSource(results, 0, 0L);
            assertSameSource(results, 1, 1L);
            assertSameSource(results, 2, 2L);
            assertFalse(results.hasNext());
        });
    }

    @Test
    void statsTest() {
        Map<String, Object> params = map("config", map("similarityCutoff", 0.0));

        Map<String, Object> row = runQuery(STATEMENT_STATS, params, Result::next);
        assertEquals((double) row.get("p25"), 1.0, 0.01);
        assertEquals((double) row.get("p50"), 1.0, 0.01);
        assertEquals((double) row.get("p75"), 1.0, 0.01);
        assertEquals((double) row.get("p95"), 1.0, 0.01);
        assertEquals((double) row.get("p99"), 1.0, 0.01);
        assertEquals((double) row.get("p100"), 1.0, 0.01);
    }

    @Test
    void simpleoverlapTest() {
        Map<String, Object> params = map("config", map("similarityCutoff", 0.0));

        Map<String, Object> row = runQuery(STATEMENT, params, Result::next);
        assertEquals((double) row.get("p25"), 1.0, 0.01);
        assertEquals((double) row.get("p50"), 1.0, 0.01);
        assertEquals((double) row.get("p75"), 1.0, 0.01);
        assertEquals((double) row.get("p95"), 1.0, 0.01);
        assertEquals((double) row.get("p99"), 1.0, 0.01);
        assertEquals((double) row.get("p100"), 1.0, 0.01);
    }

    @Test
    void simpleoverlapFromEmbeddingTest() {
        runQuery(STORE_EMBEDDING_STATEMENT);

        Map<String, Object> params = map("config", map("similarityCutoff", 0.0));

        Map<String, Object> row = runQuery(EMBEDDING_STATEMENT, params, Result::next);
        assertEquals((double) row.get("p25"), 1.0, 0.01);
        assertEquals((double) row.get("p50"), 1.0, 0.01);
        assertEquals((double) row.get("p75"), 1.0, 0.01);
        assertEquals((double) row.get("p95"), 1.0, 0.01);
        assertEquals((double) row.get("p99"), 1.0, 0.01);
        assertEquals((double) row.get("p100"), 1.0, 0.01);
    }

    /*
    Alice       [p1,p2,p3]
    Bob         [p1,p2]
    Charlie     [p3]
    Dana        []
     */

    @Test
    void simpleoverlapWriteTest() {
        Map<String, Object> params = map("config", map("similarityCutoff", 0.1));

        runQuery(STATEMENT, params);

        String checkSimilaritiesQuery = "MATCH (a)-[similar:NARROWER_THAN]->(b)" +
                                        "RETURN a.name AS node1, b.name AS node2, similar.score AS score " +
                                        "ORDER BY id(a), id(b)";

        runQueryWithResultConsumer(checkSimilaritiesQuery, result -> {
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals(row.get("node1"), "Bob");
            assertEquals(row.get("node2"), "Alice");
            assertEquals((double) row.get("score"), 1.0, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Charlie");
            assertEquals(row.get("node2"), "Alice");
            assertEquals((double) row.get("score"), 1.0, 0.01);

            assertFalse(result.hasNext());
        });
    }

    @Test
    void dontComputeComputationsByDefault() {
        Map<String, Object> params = map("config", map(
            "similarityCutoff", 0.1
        ));

        Map<String, Object> writeRow = runQuery(STATEMENT, params, Result::next);
        assertEquals(-1L, (long) writeRow.get("computations"));
    }

    @Test
    void numberOfComputations() {
        Map<String, Object> params = map("config", map(
            "showComputations", true,
            "similarityCutoff", 0.1
        ));

        Map<String, Object> writeRow = runQuery(STATEMENT, params, Result::next);
        assertEquals(3L, (long) writeRow.get("computations"));
    }

    private void assert12(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(1L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(0D, row.get("similarity"));
    }

    // a / b = 2 : 2/3
    // a / c = 1 : 1/3
    // b / c = 0 : 0/3 = 0

    private void assert20(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(0L, row.get("item2"));
        assertEquals(1L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        // assertEquals(1L, row.get("intersection"));
        assertEquals(1D / 1D, row.get("similarity"));
    }

    private void assert10(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(0L, row.get("item2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(2D / 2D, row.get("similarity"));
    }

    @Override
    Class<? extends AlphaSimilarityProc<OverlapAlgorithm, ? extends SimilarityConfig>> getProcedureClazz() {
        return OverlapProc.class;
    }
}
