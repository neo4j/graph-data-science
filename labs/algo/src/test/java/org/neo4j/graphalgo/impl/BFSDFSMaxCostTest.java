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

package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Graph:
 *
 * a-(2)--c
 * | \    |
 * (3)(9) (1)
 * |   \  |
 * b-(9)--d-(1)-e
 */
class BFSDFSMaxCostTest extends AlgoTestBase {

    private static Graph graph;

    private static List<String> dfsExpected = Arrays
        .stream(new int[]{1, 3, 4, 5, 2})
        .mapToObj(Objects::toString)
        .collect(Collectors.toList());

    @BeforeEach
    void setupGraph() {
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

        graph = new GraphLoader(db)
            .withAnyRelationshipType()
            .withAnyLabel()
            .withRelationshipProperties(PropertyMapping.of("distance", Double.MAX_VALUE))
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    @Test
    void testDfsMaxCostOut() {
        final long source = id("a");
        final double maxCost = 5.;
        final long[] nodes = new Traverse(graph)
            .computeDfs(
                source,
                Direction.OUTGOING,
                (s, t, w) -> {
                    Traverse.ExitPredicate.Result result = w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
                    System.out.println("Exit Function: " + name(s) + " -(" + w + ")-> " + name(t) + " --> " + result);
                    return result;
                }, (s, t, w) -> {
                    final double v = graph.relationshipProperty(s, t, 0.0D);
                    System.out.println("Aggregator: " + name(s) + " -(" + (w + v) + ")-> " + name(t));
                    return w + v;
                }
            );

        try (Transaction tx = db.beginTx()) {
            List<Node> resultNodes = Arrays
                .stream(nodes)
                .mapToObj(nodeId -> db.getNodeById(nodeId))
                .collect(Collectors.toList());
            List<String> explicitIds = resultNodes
                .stream()
                .map(node -> node.getProperty("id"))
                .map(Objects::toString)
                .collect(Collectors.toList());
            System.out.println(explicitIds);

            assertEquals(dfsExpected.size(), explicitIds.size());
            explicitIds.forEach(id -> assertTrue(dfsExpected.contains(id)));

            tx.success();
        }
    }

    @Disabled("TODO: Double check the BFS functionality, it doesn't seem to be correct")
    @Test
    void testBfsMaxCostOut() {
        final long source = id("a");
        final double maxCost = 5.;
        final long[] nodes = new Traverse(graph)
            .computeBfs(
                source,
                Direction.OUTGOING,
                (s, t, w) -> {
                    Traverse.ExitPredicate.Result result = w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
                    System.out.println("Exit Function: " + name(s) + " -(" + w + ")-> " + name(t) + " --> " + result);
                    return result;
                }, (s, t, w) -> {
                    final double v = graph.relationshipProperty(s, t, 0.0D);
                    System.out.println("Aggregator: " + name(s) + " -(" + (w + v) + ")-> " + name(t));
                    return w + v;
                }
            );

        final Collection<String> names = new HashSet<>(5);
        try (Transaction tx = db.beginTx()) {
            List<Node> resultNodes = Arrays
                .stream(nodes)
                .mapToObj(nodeId -> db.getNodeById(nodeId))
                .collect(Collectors.toList());
            List<String> explicitIds = resultNodes
                .stream()
                .peek(node -> names.add(name(node.getId())))
                .map(node -> node.getProperty("id"))
                .map(Objects::toString)
                .collect(Collectors.toList());
            System.out.println(String.join(", ", names));

            assertEquals(dfsExpected.size(), explicitIds.size());
            explicitIds.forEach(id -> assertTrue(dfsExpected.contains(id)));

            tx.success();
        }
    }

    private String name(long nodeId) {
        try (Transaction tx = db.beginTx()) {
            final Node nodeById = db.getNodeById(nodeId);
            tx.success();

            return nodeById.getProperty("name").toString();
        }
    }

    private long id(String name) {
        final Node[] node = new Node[1];
        runQuery("MATCH (n:Place) WHERE n.name = '" + name + "' RETURN n", row -> node[0] = row.getNode("n"));
        return node[0].getId();
    }
}
