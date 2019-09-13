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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public final class ShortestPathDijkstraTest {

    // https://en.wikipedia.org/wiki/Shortest_path_problem#/media/File:Shortest_path_with_direct_weights.svg
    private static final String DB_CYPHER = "" +
            "CREATE (a:Label1 {name:\"a\"})\n" +
            "CREATE (b:Label1 {name:\"b\"})\n" +
            "CREATE (c:Label1 {name:\"c\"})\n" +
            "CREATE (d:Label1 {name:\"d\"})\n" +
            "CREATE (e:Label1 {name:\"e\"})\n" +
            "CREATE (f:Label1 {name:\"f\"})\n" +
            "CREATE\n" +
            "  (a)-[:TYPE1 {cost:4}]->(b),\n" +
            "  (a)-[:TYPE1 {cost:2}]->(c),\n" +
            "  (b)-[:TYPE1 {cost:5}]->(c),\n" +
            "  (b)-[:TYPE1 {cost:10}]->(d),\n" +
            "  (c)-[:TYPE1 {cost:3}]->(e),\n" +
            "  (d)-[:TYPE1 {cost:11}]->(f),\n" +
            "  (e)-[:TYPE1 {cost:4}]->(d),\n" +
            "  (a)-[:TYPE2 {cost:1}]->(d),\n" +
            "  (b)-[:TYPE2 {cost:1}]->(f)\n";

    // https://www.cise.ufl.edu/~sahni/cop3530/slides/lec326.pdf
    // without the additional 14 edge
    private static final String DB_CYPHER2 = "" +
            "CREATE (n1:Label2 {name:\"1\"})\n" +
            "CREATE (n2:Label2 {name:\"2\"})\n" +
            "CREATE (n3:Label2 {name:\"3\"})\n" +
            "CREATE (n4:Label2 {name:\"4\"})\n" +
            "CREATE (n5:Label2 {name:\"5\"})\n" +
            "CREATE (n6:Label2 {name:\"6\"})\n" +
            "CREATE (n7:Label2 {name:\"7\"})\n" +
            "CREATE\n" +
            "  (n1)-[:TYPE2 {cost:6}]->(n2),\n" +
            "  (n1)-[:TYPE2 {cost:2}]->(n3),\n" +
            "  (n1)-[:TYPE2 {cost:16}]->(n4),\n" +
            "  (n2)-[:TYPE2 {cost:4}]->(n5),\n" +
            "  (n2)-[:TYPE2 {cost:5}]->(n4),\n" +
            "  (n3)-[:TYPE2 {cost:7}]->(n2),\n" +
            "  (n3)-[:TYPE2 {cost:3}]->(n5),\n" +
            "  (n3)-[:TYPE2 {cost:8}]->(n6),\n" +
            "  (n4)-[:TYPE2 {cost:7}]->(n3),\n" +
            "  (n5)-[:TYPE2 {cost:4}]->(n4),\n" +
            "  (n5)-[:TYPE2 {cost:10}]->(n7),\n" +
            "  (n6)-[:TYPE2 {cost:1}]->(n7)\n";

    private static final String DB_CYPHER_599 = "" +
            "CREATE (n1:Label599 {id:\"1\"})\n" +
            "CREATE (n2:Label599 {id:\"2\"})\n" +
            "CREATE (n3:Label599 {id:\"3\"})\n" +
            "CREATE (n4:Label599 {id:\"4\"})\n" +
            "CREATE (n5:Label599 {id:\"5\"})\n" +
            "CREATE (n6:Label599 {id:\"6\"})\n" +
            "CREATE (n7:Label599 {id:\"7\"})\n" +
            "CREATE\n" +
            "  (n1)-[:TYPE599 {cost:0.5}]->(n2),\n" +
            "  (n1)-[:TYPE599 {cost:5.0}]->(n3),\n" +
            "  (n2)-[:TYPE599 {cost:0.5}]->(n5),\n" +
            "  (n3)-[:TYPE599 {cost:2.0}]->(n4),\n" +
            "  (n5)-[:TYPE599 {cost:0.5}]->(n6),\n" +
            "  (n6)-[:TYPE599 {cost:0.5}]->(n3),\n" +
            "  (n6)-[:TYPE599 {cost:23.0}]->(n7),\n" +
            "  (n1)-[:TYPE599 {cost:5.0}]->(n4)";

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{GraphViewFactory.class, "GraphViewFactory"},
                new Object[]{HugeGraphFactory.class, "HugeGraphFactory"}
        );
    }

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() {
        DB.execute(DB_CYPHER).close();
        DB.execute(DB_CYPHER2).close();
        DB.execute(DB_CYPHER_599).close();
    }

    private Class<? extends GraphFactory> graphImpl;



    public ShortestPathDijkstraTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        this.graphImpl = graphImpl;
    }

    @Test
    public void test1() {
        final Label label = Label.label("Label1");
        RelationshipType type = RelationshipType.withName("TYPE1");

        ShortestPath expected = expected(label, type,
                "name", "a",
                "name", "c",
                "name", "e",
                "name", "d",
                "name", "f");
        long[] nodeIds = expected.nodeIds;

        final Graph graph = new GraphLoader(DB)
                .withLabel(label)
                .withRelationshipType(type)
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.OUTGOING)
                .load(graphImpl);

        final ShortestPathDijkstra shortestPathDijkstra = new ShortestPathDijkstra(graph);
        shortestPathDijkstra.compute(nodeIds[0], nodeIds[nodeIds.length - 1], Direction.OUTGOING);
        final long[] path = Arrays.stream(shortestPathDijkstra.getFinalPath().toArray()).mapToLong(graph::toOriginalNodeId).toArray();

        assertEquals(expected.weight, shortestPathDijkstra.getTotalCost(), 0.1);
        assertArrayEquals(nodeIds, path);
    }

    @Test
    public void test2() {
        final Label label = Label.label("Label2");
        RelationshipType type = RelationshipType.withName("TYPE2");
        ShortestPath expected = expected(label, type,
                "name", "1",
                "name", "3",
                "name", "6",
                "name", "7");
        long[] nodeIds = expected.nodeIds;

        final Graph graph = new GraphLoader(DB)
                .withLabel(label)
                .withRelationshipType(type)
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.OUTGOING)
                .load(graphImpl);

        final ShortestPathDijkstra shortestPathDijkstra = new ShortestPathDijkstra(graph);
        shortestPathDijkstra.compute(nodeIds[0], nodeIds[nodeIds.length - 1], Direction.OUTGOING);
        final long[] path = Arrays.stream(shortestPathDijkstra.getFinalPath().toArray()).mapToLong(graph::toOriginalNodeId).toArray();

        assertEquals(expected.weight, shortestPathDijkstra.getTotalCost(), 0.1);
        assertArrayEquals(nodeIds, path);
    }

    /** @see <a href="https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/599">Issue #599</a> */
    @Test
    public void test599() {
        Label label = Label.label("Label599");
        RelationshipType type = RelationshipType.withName("TYPE599");
        ShortestPath expected = expected(
                label, type,
                "id", "1", "id", "2", "id", "5",
                "id", "6", "id", "3", "id", "4");

        Graph graph = new GraphLoader(DB)
                .withLabel(label)
                .withRelationshipType(type)
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.OUTGOING)
                .load(graphImpl);

        ShortestPathDijkstra shortestPathDijkstra = new ShortestPathDijkstra(graph);
        shortestPathDijkstra.compute(
                expected.nodeIds[0],
                expected.nodeIds[expected.nodeIds.length - 1],
                Direction.OUTGOING
        );
        long[] path = Arrays
                .stream(shortestPathDijkstra.getFinalPath().toArray())
                .mapToLong(graph::toOriginalNodeId)
                .toArray();

        assertArrayEquals(expected.nodeIds, path);
        assertEquals(expected.weight, shortestPathDijkstra.getTotalCost(), 0.1);
    }

    @Test
    public void testResultStream() {
        final Label label = Label.label("Label1");
        RelationshipType type = RelationshipType.withName("TYPE1");
        ShortestPath expected = expected(label, type,
                "name", "a",
                "name", "c",
                "name", "e",
                "name", "d",
                "name", "f");
        final long head = expected.nodeIds[0], tail = expected.nodeIds[expected.nodeIds.length - 1];

        final Graph graph = new GraphLoader(DB)
                .withLabel(label)
                .withRelationshipType("TYPE1")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .withDirection(Direction.OUTGOING)
                .load(graphImpl);

        final ShortestPathDijkstra shortestPathDijkstra = new ShortestPathDijkstra(graph);
        Stream<ShortestPathDijkstra.Result> resultStream = shortestPathDijkstra
                .compute(head, tail, Direction.OUTGOING)
                .resultStream();

        assertEquals(expected.weight, shortestPathDijkstra.getTotalCost(), 0.1);
        assertEquals(expected.nodeIds.length, resultStream.count());
    }

    private static ShortestPath expected(
            Label label,
            RelationshipType type,
            String... kvPairs) {
        return DB.executeAndCommit(db -> {
            double weight = 0.0;
            Node prev = null;
            long[] nodeIds = new long[kvPairs.length / 2];
            for (int i = 0; i < nodeIds.length; i++) {
                Node current = db.findNode(label, kvPairs[2*i], kvPairs[2*i + 1]);
                long id = current.getId();
                nodeIds[i] = id;
                if (prev != null) {
                    for (Relationship rel : prev.getRelationships(type, Direction.OUTGOING)) {
                        if (rel.getEndNodeId() == id) {
                            double cost = ((Number) rel.getProperty("cost")).doubleValue();
                            weight += cost;
                        }
                    }
                }
                prev = current;
            }

            return new ShortestPath(nodeIds, weight);
        });
    }

    private static final class ShortestPath {
        private final long[] nodeIds;
        private final double weight;

        private ShortestPath(long[] nodeIds, double weight) {
            this.nodeIds = nodeIds;
            this.weight = weight;
        }
    }
}
