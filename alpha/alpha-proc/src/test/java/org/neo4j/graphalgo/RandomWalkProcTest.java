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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.walking.RandomWalkProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RandomWalkProcTest extends BaseProcTest {

    private static final int NODE_COUNT = 54;

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a'})" +
        ", (b:Fred {name:'b'})" +
        ", (c:Fred {name:'c'})" +
        ", (d:Bob {name:'d'})" +
        ", (a)-[:OF_TYPE {cost:5, blue: 1}]->(b)" +
        ", (a)-[:OF_TYPE {cost:10, blue: 1}]->(c)" +
        ", (c)-[:DIFFERENT {cost:2, blue: 0}]->(b)" +
        ", (b)-[:OF_TYPE {cost:5, blue: 1}]->(c)" +
        "  WITH * UNWIND range(0, $count - 1) AS id " +
        "  CREATE (n:Node {name: '' + id})" +
        "  CREATE " +
        "    (n)-[:OF_TYPE {cost: 5, blue: 1}]->(a)," +
        "    (b)-[:OF_TYPE {cost: 5, blue: 1}]->(n)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(RandomWalkProc.class);
        runQuery(DB_CYPHER, Collections.singletonMap("count", NODE_COUNT - 4));
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void shouldHaveGivenStartNodeRandom() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("start", 1)
            .addParameter("steps", 1)
            .addParameter("walks", 1)
            .yields();

        try (ResourceIterator<List<Long>> result = runQuery(
            query,
            r -> r.columnAs("nodeIds")
        )) {
            List<Long> path = result.next();
            assertEquals(1L, path.get(0).longValue());
            assertNotEquals(1L, path.get(1).longValue());
        }
    }

    @Test
    void shouldHaveResultsRandom() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("steps", 1)
            .addParameter("walks", 5)
            .yields();

        assertTrue(runQuery(query, Result::hasNext));
    }

    @Test
    void shouldHaveSameTypesForStartNodesRandom() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("start", "Fred")
            .addParameter("steps", 2)
            .addParameter("walks", 5)
            .addParameter("path", true)
            .yields();

        // TODO: make this test predictable (i.e. set random seed)
        int count = runQuery(query, r -> {
            ResourceIterator<Path> results = r.columnAs("path");
            int resultCount = 0;
            while (results.hasNext()) {
                Path path = results.next();
                assertTrue(path.startNode().hasLabel(Label.label("Fred")), "Nodes should be of type 'Fred'.");
                assertEquals(2, path.length());
                Relationship firstRel = path.relationships().iterator().next();
                assertEquals(path.startNode(), firstRel.getStartNode());
                resultCount++;
            }
            return resultCount;
        });
        assertEquals(5, count);
    }

    @Test
    void shouldHaveStartedFromEveryNodeRandom() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("steps", 1)
            .addParameter("walks", 100)
            .yields();

        ResourceIterator<List<Long>> results = runQuery(query, r -> r.columnAs("nodeIds"));

        Set<Long> nodeIds = new HashSet<>();
        while (results.hasNext()) {
            List<Long> path = results.next();
            nodeIds.add(path.get(0));
        }
        assertEquals(NODE_COUNT, nodeIds.size(), "Should have visited all nodes.");
    }

    @Test
    void shouldNotFailRandom() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("start", 2)
            .addParameter("steps", 7)
            .addParameter("walks", 2)
            .yields();

        Boolean noMoreNext = runQuery(query, r -> {
            r.next();
            r.next();

            return r.hasNext();
        });

        assertTrue(!noMoreNext, "There should be only two results.");
    }

    @Test
    void shouldHaveGivenStartNode() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("start", 1)
            .addParameter("steps", 1)
            .addParameter("walks", 1)
            .addParameter("mode", "node2vec")
            .addParameter("return", 1)
            .addParameter("inOut", 1)
            .yields();

        List<Long> result = runQuery(query, r -> r.<List<Long>>columnAs("nodeIds").next());
        assertThat(1L, equalTo(result.get(0)));
    }

    @Test
    void shouldHaveResultsN2V() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("steps", 1)
            .addParameter("walks", 5)
            .addParameter("mode", "node2vec")
            .addParameter("return", 1)
            .addParameter("inOut", 1)
            .yields();

        ResourceIterator<List<Long>> results = runQuery(query, r -> r.columnAs("nodeIds"));

        assertTrue(results.hasNext());
    }

    @Test
    void shouldHaveStartedFromEveryNodeN2V() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("steps", 1)
            .addParameter("walks", 100)
            .addParameter("mode", "node2vec")
            .addParameter("return", 1)
            .addParameter("inOut", 1)
            .yields();

        ResourceIterator<List<Long>> results = runQuery(query, r -> r.columnAs("nodeIds"));

        Set<Long> nodeIds = new HashSet<>();
        while (results.hasNext()) {
            List<Long> record = results.next();
            nodeIds.add(record.get(0));
        }
        assertEquals(NODE_COUNT, nodeIds.size(), "Should have visited all nodes.");
    }

    @Test
    void shouldNotFailN2V() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "randomWalk")
            .streamMode()
            .addParameter("start", 2)
            .addParameter("steps", 7)
            .addParameter("walks", 2)
            .addParameter("mode", "node2vec")
            .addParameter("return", 1)
            .addParameter("inOut", 1)
            .yields();

        ResourceIterator<List<Long>> results = runQuery(query, r -> r.columnAs("nodeIds"));

        results.next();
        results.next();
        assertTrue(!results.hasNext(), "There should be only two results.");
    }
}
