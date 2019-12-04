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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Node2VecWalkProcTest extends ProcTestBase {

    private static final int NODE_COUNT = 54;

    private Transaction tx;

    @BeforeEach
    void beforeClass() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(NodeWalkerProc.class);
        runQuery(buildDatabaseQuery(), Collections.singletonMap("count",NODE_COUNT-4));
        tx = db.beginTx();
    }

    @AfterEach
    void AfterClass() {
        tx.close();
        db.shutdown();
    }

    private static String buildDatabaseQuery() {
        return "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Fred {name:'b'})\n" +
                "CREATE (c:Fred {name:'c'})\n" +
                "CREATE (d:Bob {name:'d'})\n" +

                "CREATE" +
                " (a)-[:OF_TYPE {cost:5, blue: 1}]->(b),\n" +
                " (a)-[:OF_TYPE {cost:10, blue: 1}]->(c),\n" +
                " (c)-[:DIFFERENT {cost:2, blue: 0}]->(b),\n" +
                " (b)-[:OF_TYPE {cost:5, blue: 1}]->(c) " +

                " WITH * UNWIND range(0,$count-1) AS id CREATE (n:Node {name:''+id})\n" +
                "CREATE (n)-[:OF_TYPE {cost:5, blue: 1}]->(a),\n" +
                "(b)-[:OF_TYPE {cost:5, blue: 1}]->(n)\n";
    }

    @Test
    void shouldHaveGivenStartNodeRandom() {
        try (ResourceIterator<List<Long>> result = runQuery("CALL algo.randomWalk.stream(1, 1, 1)").columnAs("nodeIds")) {
            List<Long> path = result.next();
            assertEquals(1L, path.get(0).longValue());
            assertNotEquals(1L, path.get(1).longValue());
        }
    }

    @Test
    @Timeout(100)
    void shouldHaveResultsRandom() {
        Result results = runQuery("CALL algo.randomWalk.stream(null, 1, 5)");
        assertTrue(results.hasNext());
    }

    @Test
    void shouldHandleLargeResults() {
        Result results = runQuery("CALL algo.randomWalk.stream(null, 100, 100000)");
        assertEquals(100000,Iterators.count(results));
    }

    @Test
    void shouldHaveSameTypesForStartNodesRandom() {
        // TODO: make this test predictable (i.e. set random seed)
        ResourceIterator<Path> results = runQuery("CALL algo.randomWalk.stream('Fred', 2, 5,{path:true})").columnAs("path");
        int count = 0;
        while (results.hasNext()) {
            Path path = results.next();
            assertTrue(path.startNode().hasLabel(Label.label("Fred")), "Nodes should be of type 'Fred'.");
            assertEquals(2, path.length());
            Relationship firstRel = path.relationships().iterator().next();
            assertEquals(path.startNode(), firstRel.getStartNode());
            count ++;
        }
        assertEquals(5, count);
    }

    @Test
    @Timeout(200)
    void shouldHaveStartedFromEveryNodeRandom() {
        ResourceIterator<List<Long>> results = runQuery("CALL algo.randomWalk.stream(null,1,100)").columnAs("nodeIds");

        Set<Long> nodeIds = new HashSet<>();
        while (results.hasNext()) {
            List<Long> path = results.next();
            nodeIds.add(path.get(0));
        }
        assertEquals(NODE_COUNT, nodeIds.size(), "Should have visited all nodes.");
    }

    @Test
    void shouldNotFailRandom() {
        Result results = runQuery("CALL algo.randomWalk.stream(2, 7, 2)");

        results.next();
        results.next();
        assertTrue(!results.hasNext(), "There should be only two results.");
    }

    @Test
    void shouldHaveGivenStartNode() {
        List<Long> result = runQuery("CALL algo.randomWalk.stream(1, 1, 1, {mode:'node2vec', return: 1, inOut:1})").<List<Long>>columnAs("nodeIds").next();
        assertThat( 1L, equalTo(result.get(0)));
    }

    @Test
    @Timeout(100)
    void shouldHaveResultsN2V() {
        ResourceIterator<List<Long>> results = runQuery("CALL algo.randomWalk.stream(null, 1, 5, {mode:'node2vec', return: 1, inOut:1})").columnAs("nodeIds");

        assertTrue(results.hasNext());
    }

    @Test
    void shouldHaveStartedFromEveryNodeN2V() {
        ResourceIterator<List<Long>> results = runQuery("CALL algo.randomWalk.stream(null, 1, 100, {mode:'node2vec', return: 1, inOut:1})").columnAs("nodeIds");

        Set<Long> nodeIds = new HashSet<>();
        while (results.hasNext()) {
            List<Long> record = results.next();
            nodeIds.add(record.get(0));
        }
        assertEquals(NODE_COUNT,  nodeIds.size(), "Should have visited all nodes.");
    }

    @Test
    void shouldNotFailN2V() {
        ResourceIterator<List<Long>> results = runQuery("CALL algo.randomWalk.stream(2, 7, 2, {mode:'node2vec', return: 1, inOut:1})").columnAs("nodeIds");

        results.next();
        results.next();
        assertTrue(!results.hasNext(), "There should be only two results.");
    }
}
