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
import org.neo4j.graphalgo.OverlapProc;
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

public class OverlapTest {

    private static GraphDatabaseAPI db;
    private Transaction tx;
    public static final String STATEMENT_STREAM = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
            "WITH {item:id(p), categories: collect(distinct id(i))} as userData\n" +
            "WITH collect(userData) as data\n" +
            "call algo.similarity.overlap.stream(data,$config) " +
            "yield item1, item2, count1, count2, intersection, similarity " +
            "RETURN item1, item2, count1, count2, intersection, similarity " +
            "ORDER BY item1,item2";

    public static final String STATEMENT = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
            "WITH {item:id(p), categories: collect(distinct id(i))} as userData\n" +
            "WITH collect(userData) as data\n" +
            "CALL algo.similarity.overlap(data, $config) " +
            "yield p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations " +
            "RETURN p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations";

    public static final String STORE_EMBEDDING_STATEMENT = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
            "WITH p, collect(distinct id(i)) as userData\n" +
            "SET p.embedding = userData";

    public static final String EMBEDDING_STATEMENT = "MATCH (p:Person) \n" +
            "WITH {item:id(p), categories: p.embedding} as userData\n" +
            "WITH collect(userData) as data\n" +
            "CALL algo.similarity.overlap(data, $config) " +
            "yield p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs " +
            "RETURN p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs";

    @BeforeClass
    public static void beforeClass() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(OverlapProc.class);
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
                "MERGE (p)-[:LIKES]->(i) RETURN count(*) ";
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

                "CREATE" +
                " (a)-[:LIKES]->(i1),\n" +
                " (a)-[:LIKES]->(i2),\n" +
                " (a)-[:LIKES]->(i3),\n" +
                " (b)-[:LIKES]->(i1),\n" +
                " (b)-[:LIKES]->(i2),\n" +
                " (c)-[:LIKES]->(i3)\n";
        // a: 3
        // b: 2
        // c: 1
        // a / b = 2 : 2/3
        // a / c = 1 : 1/3
        // b / c = 0 : 0/3 = 0
    }


    @Test
    public void overlapSingleMultiThreadComparision() {
        int size = 333;
        buildRandomDB(size);
        Result result1 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"concurrency", 1)));
        Result result2 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"concurrency", 2)));
        Result result4 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"concurrency", 4)));
        Result result8 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"concurrency", 8)));
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
    public void overlapSingleMultiThreadComparisionTopK() {
        int size = 333;
        buildRandomDB(size);

        Result result1 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 1)));
        Result result2 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 2)));
        Result result4 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 4)));
        Result result8 = db.execute(STATEMENT_STREAM, map("config", map("similarityCutoff",-0.1,"topK",1,"concurrency", 8)));
        int count=0;
        while (result1.hasNext()) {
            Map<String, Object> row1 = result1.next();
            assertEquals(row1.toString(), row1,result2.next());
            assertEquals(row1.toString(), row1,result4.next());
            assertEquals(row1.toString(), row1,result8.next());
            count++;
        }
        assertFalse(result2.hasNext());
        assertFalse(result4.hasNext());
        assertFalse(result8.hasNext());
    }

    @Test
    public void topNoverlapStreamTest() {
        Result results = db.execute(STATEMENT_STREAM, map("config",map("top",2)));
        assert10(results.next());
        assert20(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void overlapStreamTest() {
        Result results = db.execute(STATEMENT_STREAM, map("config",map("concurrency",1)));

        assertTrue(results.hasNext());
        assert10(results.next());
        assert20(results.next());
        assert12(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void overlapStreamSourceTargetIdsTest() {
//        Map<String, Object> config = map(
//                "concurrency", 1,
//                "sourceIds", Collections.singletonList(1L),
//                "targetIds", Collections.singletonList(0L)
//        );

        Map<String, Object> config = map(
                "concurrency", 1,
                "sourceIds", Collections.singletonList(1L)
        );

        Map<String, Object> params = map("config", config);

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);

        assertTrue(results.hasNext());
        assert10(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void topKoverlapStreamTest() {
        Map<String, Object> params = map("config", map( "concurrency", 1,"topK", 1));
        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertTrue(results.hasNext());
        assert10(results.next());
        assert20(results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void topKoverlapSourceTargetIdsStreamTest() {
        Map<String, Object> config = map(
                "concurrency", 1,
                "topK", 1,
                "sourceIds", Collections.singletonList(1L)

        );
        Map<String, Object> params = map("config", config);
        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertTrue(results.hasNext());
        assert10(results.next());
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
    public void topK4overlapStreamTest() {
        Map<String, Object> params = map("config", map("topK", 4, "concurrency", 4, "similarityCutoff", -0.1));
        System.out.println(db.execute(STATEMENT_STREAM,params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM,params);
        assertSameSource(results, 0, 0L);
        assertSameSource(results, 1, 1L);
        assertSameSource(results, 2, 2L);
        assertFalse(results.hasNext());
    }

    @Test
    public void topK3overlapStreamTest() {
        Map<String, Object> params = map("config", map("concurrency", 3, "topK", 3));

        System.out.println(db.execute(STATEMENT_STREAM, params).resultAsString());

        Result results = db.execute(STATEMENT_STREAM, params);
        assertSameSource(results, 0, 0L);
        assertSameSource(results, 1, 1L);
        assertSameSource(results, 2, 2L);
        assertFalse(results.hasNext());
    }

    @Test
    public void simpleoverlapTest() {
        Map<String, Object> params = map("config", map("similarityCutoff", 0.0));

        Map<String, Object> row = db.execute(STATEMENT,params).next();
        assertEquals((double) row.get("p25"), 1.0, 0.01);
        assertEquals((double) row.get("p50"), 1.0, 0.01);
        assertEquals((double) row.get("p75"), 1.0, 0.01);
        assertEquals((double) row.get("p95"), 1.0, 0.01);
        assertEquals((double) row.get("p99"), 1.0, 0.01);
        assertEquals((double) row.get("p100"), 1.0, 0.01);
    }

    @Test
    public void simpleoverlapFromEmbeddingTest() {
        db.execute(STORE_EMBEDDING_STATEMENT);

        Map<String, Object> params = map("config", map("similarityCutoff", 0.0));

        Map<String, Object> row = db.execute(EMBEDDING_STATEMENT,params).next();
        System.out.println("row = " + row);
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
    public void simpleoverlapWriteTest() {
        Map<String, Object> params = map("config", map( "write",true, "similarityCutoff", 0.1));

        db.execute(STATEMENT,params).close();

        String checkSimilaritiesQuery = "MATCH (a)-[similar:NARROWER_THAN]->(b)" +
                "RETURN a.name AS node1, b.name as node2, similar.score AS score " +
                "ORDER BY id(a), id(b)";

        System.out.println(db.execute(checkSimilaritiesQuery).resultAsString());
        Result result = db.execute(checkSimilaritiesQuery);

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
        assertEquals(3L, (long) writeRow.get("computations"));
    }

    private void assert12(Map<String, Object> row) {
        assertEquals(2L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(1L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(0d, row.get("similarity"));
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
        assertEquals(1d/1d, row.get("similarity"));
    }

    private void assert10(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(0L, row.get("item2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(3L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(2d/2d, row.get("similarity"));
    }
}
