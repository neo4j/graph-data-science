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

package org.neo4j.graphalgo.impl.traverse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.HugeGraphStoreFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Graph:
 *
 * a-(2)--c
 * | \    |
 * (3)(9) (1)
 * |   \  |
 * b-(9)--d-(1)-e
 */
class TraverseMaxCostTest extends AlgoTestBase {

    private Log testLog;

    private static Graph graph;

    private final String[] dfsExpected = new String[]{"a", "c", "d", "e", "b"};
    private final String[] bfsExpected = new String[]{"a", "b", "c", "d", "e"};
    private Traverse.ExitPredicate exitPredicate;
    private Traverse.Aggregator aggregator;
    private final double maxCost = 5.;

    @BeforeEach
    void setup() {
        testLog = new TestLog();
        db = TestDatabaseCreator.createTestDatabase();
        String cypher = "CREATE (a:Place {name: 'a', id:'1'})\n" +
                        "CREATE (b:Place {name: 'b', id:'2'})\n" +
                        "CREATE (c:Place {name: 'c', id:'3'})\n" +
                        "CREATE (d:Place {name: 'd', id:'4'})\n" +
                        "CREATE (e:Place {name: 'e', id:'5'})\n" +
                        "CREATE " +
                        "    (a)-[:Connection {distance:3}]->(b),\n" +
                        "    (a)-[:Connection {distance:2}]->(c),\n" +
                        "    (a)-[:Connection {distance:9}]->(d),\n" +
                        "    (c)-[:Connection {distance:1}]->(d),\n" +
                        "    (b)-[:Connection {distance:9}]->(d),\n" +
                        "    (d)-[:Connection {distance:1}]->(e)";

        runQuery(cypher);

        graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .addRelationshipProperty(PropertyMapping.of("distance", Double.MAX_VALUE))
            .build()
            .load(HugeGraphStoreFactory.class);

        exitPredicate = (s, t, w) -> {
            Traverse.ExitPredicate.Result result = w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            testLog.debug("Exit Function: " + name(s) + " -(" + w + ")-> " + name(t) + " --> " + result);
            return result;
        };
        aggregator = (s, t, w) -> {
            final double v = graph.relationshipProperty(s, t, 0.0D);
            testLog.debug("Aggregator: " + name(s) + " -(" + (w + v) + ")-> " + name(t));
            return w + v;
        };
    }

    @AfterEach
    void cleanup() {
        db.shutdown();
    }

    @Test
    void testDfsMaxCostOut() {
        long source = id("a");
        long[] nodes = Traverse.dfs(
            graph,
            source,
            exitPredicate,
            aggregator
        ).compute().resultNodes();

        try (Transaction tx = db.beginTx()) {
            List<String> resultNodeNames = Arrays
                .stream(nodes)
                .mapToObj(nodeId -> db.getNodeById(nodeId))
                .map(node -> node.getProperty("name"))
                .map(Objects::toString)
                .collect(Collectors.toList());

            testLog.debug("Result Nodes: ", resultNodeNames);

            assertThat(resultNodeNames, containsInAnyOrder(dfsExpected));

            tx.success();
        }
    }

    @Test
    void testBfsMaxCostOut() {
        long source = id("a");

        long[] nodes = Traverse.bfs(
            graph,
            source,
            exitPredicate,
            aggregator
        ).compute().resultNodes();

        try (Transaction tx = db.beginTx()) {
            List<String> resultNodeNames = Arrays
                .stream(nodes)
                .mapToObj(nodeId -> db.getNodeById(nodeId))
                .map(node -> node.getProperty("name"))
                .map(Objects::toString)
                .collect(Collectors.toList());

            testLog.debug("Result Nodes: ", resultNodeNames);

            assertThat(resultNodeNames, containsInAnyOrder(bfsExpected));

            tx.success();
        }
    }

    private String name(long nodeId) {
        try (Transaction tx = db.beginTx()) {
            Node nodeById = db.getNodeById(nodeId);
            tx.success();

            return nodeById.getProperty("name").toString();
        }
    }

    private long id(String name) {
        Node[] node = new Node[1];
        runQuery("MATCH (n:Place) WHERE n.name = '" + name + "' RETURN n", row -> node[0] = row.getNode("n"));
        return node[0].getId();
    }
}
