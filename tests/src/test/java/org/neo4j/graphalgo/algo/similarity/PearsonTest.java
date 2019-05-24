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
package org.neo4j.graphalgo.algo.similarity;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.IsFiniteFunc;
import org.neo4j.graphalgo.PearsonProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;

public class PearsonTest {

    private static GraphDatabaseAPI db;
    private Transaction tx;
    public static final String STATEMENT_STREAM = "MATCH (i:Item) WITH i ORDER BY i MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(i)\n" +
            "WITH p, i, r " +
            "ORDER BY id(p), id(i) " +
            "WITH {item:id(p), weights: collect(coalesce(r.stars,$missingValue))} as userData\n" +
            "WITH collect(userData) as data\n" +
            "call algo.similarity.pearson.stream(data,$config) " +
            "yield item1, item2, count1, count2, intersection, similarity " +
            "RETURN item1, item2, count1, count2, intersection, similarity " +
            "ORDER BY item1,item2";

    public static final String STATEMENT_CYPHER_STREAM = "call algo.similarity.pearson.stream($query,$config) " +
            "yield item1, item2, count1, count2, intersection, similarity " +
            "RETURN * ORDER BY item1,item2";

    public static final String STATEMENT = "MATCH (i:Item) WITH i ORDER BY id(i) MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(i)\n" +
            "WITH {item:id(p), weights: collect(coalesce(r.stars,0))} as userData\n" +
            "WITH collect(userData) as data\n" +

            "CALL algo.similarity.pearson(data, $config) " +
            "yield p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations " +
            "RETURN *";

    public static final String STORE_EMBEDDING_STATEMENT = "MATCH (i:Item) WITH i ORDER BY id(i) MATCH (p:Person) OPTIONAL MATCH (p)-[r:LIKES]->(i)\n" +
            "WITH p, collect(coalesce(r.stars,0)) as userData\n" +
            "SET p.embedding = userData";

    public static final String EMBEDDING_STATEMENT = "MATCH (p:Person)\n" +
            "WITH {item:id(p), weights: p.embedding} as userData\n" +
            "WITH collect(userData) as data\n" +

            "CALL algo.similarity.pearson(data, $config) " +
            "yield p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs " +
            "RETURN *";

    @BeforeClass
    public static void beforeClass() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(PearsonProc.class);
        procedures.registerFunction(IsFiniteFunc.class);
        db.execute(buildDatabaseQuery()).close();
    }

    @AfterClass
    public static void AfterClass() {
        db.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        tx = db.beginTx();
    }

    @After
    public void tearDown() throws Exception {
        tx.close();
    }

    private static void buildRandomDB(int size) {
        db.execute("MATCH (n) DETACH DELETE n").close();
        db.execute("UNWIND range(1,$size/10) as _ CREATE (:Person) CREATE (:Item) ",singletonMap("size",size)).close();
        String statement =
                "MATCH (p:Person) WITH collect(p) as people " +
                "MATCH (i:Item) WITH people, collect(i) as items " +
                "UNWIND range(1,$size) as _ " +
                "WITH people[toInteger(rand()*size(people))] as p, items[toInteger(rand()*size(items))] as i " +
                "MERGE (p)-[likes:LIKES]->(i) SET likes.stars = toInteger(rand()*$size)  RETURN count(*) ";
        db.execute(statement,singletonMap("size",size)).close();
    }
    private static String buildDatabaseQuery() {
        return  "CREATE (a:Person {name:'Alice'})\n" +
                "CREATE (b:Person {name:'Bob'})\n" +
                "CREATE (c:Person {name:'Charlie'})\n" +
                "CREATE (d:Person {name:'Dana'})\n" +

                "CREATE (i1:Item {name:'p1'})\n" +
                "CREATE (i2:Item {name:'p2'})\n" +
                "CREATE (i3:Item {name:'p3'})\n" +
                "CREATE (i4:Item {name:'p4'})\n" +

                "CREATE" +
                " (a)-[:LIKES {stars:1}]->(i1),\n" +
                " (a)-[:LIKES {stars:2}]->(i2),\n" +
                " (a)-[:LIKES {stars:3}]->(i3),\n" +
                " (a)-[:LIKES {stars:4}]->(i4),\n" +

                " (b)-[:LIKES {stars:2}]->(i1),\n" +
                " (b)-[:LIKES {stars:3}]->(i2),\n" +
                " (b)-[:LIKES {stars:4}]->(i3),\n" +
                " (b)-[:LIKES {stars:5}]->(i4),\n" +

                " (c)-[:LIKES {stars:3}]->(i1),\n" +
                " (c)-[:LIKES {stars:4}]->(i2),\n" +
                " (c)-[:LIKES {stars:4}]->(i3),\n" +
                " (c)-[:LIKES {stars:5}]->(i4),\n" +

                " (d)-[:LIKES {stars:3}]->(i2),\n" +
                " (d)-[:LIKES {stars:2}]->(i3),\n" +
                " (d)-[:LIKES {stars:5}]->(i4)\n";

        /*
            for (int i = 0; i < len; i++) {
             double weight1 = vector1[i];
             // if (weight1 == 0d) continue;
             double weight2 = vector2[i];
             // if (weight2 == 0d) continue;

             dotProduct += weight1 * weight2;
             xLength += weight1 * weight1;
             yLength += weight2 * weight2;
         }

         return dotProduct / Math.sqrt(xLength * yLength);

         */
        // Expected values via scipy's pearsonr function - https://kite.com/python/examples/656/scipy-compute-the-pearson-correlation-coefficient
        // a: 1,2,3,4
        // b: 2,3,4,5
        // c: 3,4,4,5
        // d: 0,3,2,5

        // a0 - b1: 1.0
        // a0 - c2: 0.9486832980505138
        // a0 - d3: 0.8682431421244593      0.6546536707079772
        // b1 - c2: 0.9486832980505138
        // b1 - d3: 0.8682431421244593      0.6546536707079772
        // c2 - d3: 0.9805806756909202      0.944911182523068

    }


    @Test
    public void pearsonSingleMultiThreadComparision() {
        int size = 333;
        buildRandomDB(size);

        // This is probably creating all NaN values and they're getting filtered out
//        String text = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff", -1.0, "concurrency", 1), "missingValue", 0)).resultAsString();
//        System.out.println(text);
//
//        text = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff", -1.0, "concurrency", 1), "missingValue", 0)).resultAsString();
//        System.out.println(text);

        // these can give different results because the arrays of item ids are being built in different orders
        // need to figure out a way to fix the order in the query

        System.out.println(STATEMENT_STREAM);

        Result result1 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-1.0,"concurrency", 1), "missingValue", 0));
        Result result2 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-1.0,"concurrency", 1), "missingValue", 0));
        Result result4 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-1.0,"concurrency", 1), "missingValue", 0));
        Result result8 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-1.0,"concurrency", 1), "missingValue", 0));
        int count=0;
        while (result1.hasNext()) {
            Map<String, Object> row1 = result1.next();
            assertEquals(row1.toString(), row1,result2.next());
            assertEquals(row1.toString(), row1,result4.next());
            assertEquals(row1.toString(), row1,result8.next());
            count++;
        }
        int people = size/10;
        assertEquals((people * people - people)/2,count);
    }

    @Test
    public void pearsonSingleMultiThreadComparisionTopK() {
        int size = 333;
        buildRandomDB(size);

        Result result1 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 1), "missingValue", 0));
        Result result2 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 2), "missingValue", 0));
        Result result4 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 4), "missingValue", 0));
        Result result8 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 8), "missingValue", 0));
        int count=0;
        while (result1.hasNext()) {
            Map<String, Object> row1 = result1.next();
            assertEquals(row1.toString(), row1,result2.next());
            assertEquals(row1.toString(), row1,result4.next());
            assertEquals(row1.toString(), row1,result8.next());
            count++;
        }
        int people = size/10;
        assertEquals(people,count);
    }

    @Test
    public void topNpearsonStreamTest() {
        Map<String, Object> params = map("config", map("top", 2), "missingValue", 0);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assert01(results.next());
        assert23(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void pearsonStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 1), "missingValue", 0);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertTrue(results.hasNext());
        assert01(results.next());
        assert02(results.next());
        assert03(results.next());
        assert12(results.next());
        assert13(results.next());
        assert23(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void pearsonStreamSourceTargetIdsTest() {
        Map<String, Object> config = map(
                "concurrency", 1,
                "sourceIds", Collections.singletonList(0L),
                "targetIds", Collections.singletonList(1L)
        );
        Map<String, Object> params = map("config", config, "missingValue", 0);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertTrue(results.hasNext());
        assert01(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void pearsonSkipStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 1, "skipValue", Double.NaN), "missingValue", Double.NaN);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);

        assertTrue(results.hasNext());
        assert01Skip(results.next());
        assert02Skip(results.next());
        assert03Skip(results.next());
        assert12Skip(results.next());
        assert13Skip(results.next());
        assert23Skip(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void pearsonCypherLoadingStreamTest() {
        String query = "MATCH (p:Person)-[r:LIKES]->(i) RETURN id(p) AS item, id(i) AS category, r.stars AS weight";
        Map<String, Object> params = map("config", map("concurrency", 1, "graph", "cypher", "skipValue", 0.0), "query", query);

        System.out.println(db.execute(STATEMENT_CYPHER_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_CYPHER_STREAM, params);

        assertTrue(results.hasNext());
        assert01Skip(results.next());
        assert02Skip(results.next());
        assert03Skip(results.next());
        assert12Skip(results.next());
        assert13Skip(results.next());
        assert23Skip(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void topKPearsonStreamTest() {
        Map<String, Object> params = map("config", map( "concurrency", 1,"topK", 1), "missingValue", 0);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertTrue(results.hasNext());
        assert01(results.next());
        assert01(flip(results.next()));
        assert23(results.next());
        assert23(flip(results.next()));
        assertFalse(results.hasNext());
    }

    @Test
    public void topKPearsonSourceTargetIdStreamTest() {
        Map<String, Object> config = map(
                "concurrency", 1,
                "topK", 1,
                "sourceIds", Collections.singletonList(0L)

        );
        Map<String, Object> params = map("config", config, "missingValue", 0);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertTrue(results.hasNext());
        assert01(results.next());
        assertFalse(results.hasNext());
    }

    private Map<String, Object> flip(Map<String, Object> row) {
        return map("similarity", row.get("similarity"),"intersection", row.get("intersection"),
                "item1",row.get("item2"),"count1",row.get("count2"),
                "item2",row.get("item1"),"count2",row.get("count1"));
    }

    private void assertSameSource(Result results, int count, long source) {
        Map<String, Object> row;
        long target = 0;
        for (int i = 0; i<count; i++) {
            if (target == source) target++;
            assertTrue(results.hasNext());
            row = results.next();
            assertEquals(source, row.get("item1"));
            assertEquals(target, row.get("item2"));
            target++;
        }
    }


    @Test
    public void topK4PearsonStreamTest() {
        Map<String, Object> params = map("config", map("topK", 4, "concurrency", 4, "similarityCutoff", -0.1), "missingValue", 0);

        Result results = db.execute(STATEMENT_STREAM,params);
        assertSameSource(results, 3, 0L);
        assertSameSource(results, 3, 1L);
        assertSameSource(results, 3, 2L);
        assertSameSource(results, 3, 3L);
        assertFalse(results.hasNext());
    }

    @Test
    public void topK3PearsonStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 3, "topK", 3), "missingValue", 0);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertSameSource(results, 3, 0L);
        assertSameSource(results, 3, 1L);
        assertSameSource(results, 3, 2L);
        assertSameSource(results, 3, 3L);
        assertFalse(results.hasNext());
    }

    @Test
    public void simplePearsonTest() {
        Map<String, Object> params = map("config", map());

        System.out.println(db.execute(STATEMENT, params).resultAsString());

        Map<String, Object> row = db.execute(STATEMENT,params).next();
        assertEquals(0.86,(double) row.get("p25"),  0.01);
        assertEquals(0.94, (double) row.get("p50"), 0.01);
        assertEquals(0.98,(double) row.get("p75"),  0.01);
        assertEquals(0.98,(double) row.get("p90"),  0.01);
        assertEquals(1.0,(double) row.get("p95"),  0.01);
        assertEquals(1.0,(double) row.get("p99"),  0.01);
        assertEquals( 1.0, (double) row.get("p100"), 0.01);
    }

    @Test
    public void simplePearsonFromEmbeddingTest() {
        db.execute(STORE_EMBEDDING_STATEMENT);

        Map<String, Object> params = map("config", map());
        Map<String, Object> row = db.execute(EMBEDDING_STATEMENT,params).next();

        assertEquals(0.86,(double) row.get("p25"),  0.01);
        assertEquals(0.94, (double) row.get("p50"), 0.01);
        assertEquals(0.98,(double) row.get("p75"),  0.01);
        assertEquals(0.98,(double) row.get("p90"),  0.01);
        assertEquals(1.0,(double) row.get("p95"),  0.01);
        assertEquals(1.0,(double) row.get("p99"),  0.01);
        assertEquals( 1.0, (double) row.get("p100"), 0.01);
    }

    @Test
    public void simplePearsonWriteTest() {
        Map<String, Object> params = map("config", map( "write",true, "similarityCutoff", 0.1));

        db.execute(STATEMENT,params).close();

        String checkSimilaritiesQuery = "MATCH (a)-[similar:SIMILAR]->(b)" +
                "RETURN a.name AS node1, b.name as node2, similar.score AS score " +
                "ORDER BY id(a), id(b)";

        System.out.println(db.execute(checkSimilaritiesQuery).resultAsString());
        Result result = db.execute(checkSimilaritiesQuery);

        assertTrue(result.hasNext());
        Map<String, Object> row = result.next();
        assertEquals(row.get("node1"), "Alice");
        assertEquals(row.get("node2"), "Bob");
        assertEquals((double) row.get("score"), 1.0, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Alice");
        assertEquals(row.get("node2"), "Charlie");
        assertEquals((double) row.get("score"), 0.94, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Alice");
        assertEquals(row.get("node2"), "Dana");
        assertEquals((double) row.get("score"), 0.86, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Bob");
        assertEquals(row.get("node2"), "Charlie");
        assertEquals((double) row.get("score"), 0.94, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Bob");
        assertEquals(row.get("node2"), "Dana");
        assertEquals((double) row.get("score"), 0.86, 0.01);

        assertTrue(result.hasNext());
        row = result.next();
        assertEquals(row.get("node1"), "Charlie");
        assertEquals(row.get("node2"), "Dana");
        assertEquals((double) row.get("score"), 0.98, 0.01);

        assertFalse(result.hasNext());
    }

    @Test
    public void dontComputeComputationsByDefault() {
        Map<String, Object> params = map("config", map(
                "write", true,
                "similarityCutoff", 0.1));

        Result writeResult = db.execute(STATEMENT, params);
        Map<String, Object> writeRow = writeResult.next();
        assertEquals(-1L, (long) writeRow.get("computations"));
    }

    @Test
    public void numberOfComputations() {
        Map<String, Object> params = map("config", map(
                "write", true,
                "showComputations", true,
                "similarityCutoff", 0.1));

        Result writeResult = db.execute(STATEMENT, params);
        Map<String, Object> writeRow = writeResult.next();
        assertEquals(6L, (long) writeRow.get("computations"));
    }

    private void assert23(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.98, (Double) row.get("similarity"), 0.01);
    }

    private void assert23Skip(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.77, (Double) row.get("similarity"), 0.01);
    }

    private void assert13(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.86, (Double) row.get("similarity"), 0.01);
    }

    private void assert13Skip(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.56, (Double) row.get("similarity"), 0.01);
    }

    private void assert12(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(0.948, (double) row.get("similarity"), 0.01);
    }

    private void assert12Skip(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(0.94, (Double) row.get("similarity"), 0.01);
    }

    private void assert03(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.868, (Double) row.get("similarity"), 0.01);
    }

    private void assert03Skip(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(3L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        assertEquals(0L, row.get("intersection"));
        assertEquals(0.56, (Double) row.get("similarity"), 0.01);
    }

    private void assert02(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        // assertEquals(1L, row.get("intersection"));
        assertEquals(0.94, (double)row.get("similarity"),0.01);
    }

    private void assert02Skip(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        // assertEquals(1L, row.get("intersection"));
        assertEquals(0.94, (double)row.get("similarity"),0.01);
    }

    private void assert01(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(1.0, (double)row.get("similarity"),0.01);
    }

    private void assert01Skip(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(4L, row.get("count1"));
        assertEquals(4L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(1.0, (double)row.get("similarity"),0.01);
    }
}
