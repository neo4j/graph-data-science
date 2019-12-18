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

import com.carrotsearch.hppc.procedures.IntProcedure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.shortestPath.DijkstraConfig;
import org.neo4j.graphalgo.shortestPath.ImmutableDijkstraConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

/**
 * expected path OUTGOING:  abcf
 *               INCOMING:  adef
 *               BOTH:      adef
 *
 * x should be unreachable
 *     2    2   2
 *   ,->(b)->(c)->(f)
 *  |  1    1    1 |   (x) // unreachable
 * (a)<-(d)<-(e)<-´
 */
public class DirectedDijkstraSPTest extends AlgoTestBase {

    public static final String DB_CYPHER =
        "CREATE (d:Node {name:'d'})\n" +
        "CREATE (a:Node {name:'a'})\n" +
        "CREATE (c:Node {name:'c'})\n" +
        "CREATE (b:Node {name:'b'})\n" +
        "CREATE (f:Node {name:'f'})\n" +
        "CREATE (e:Node {name:'e'})\n" +
        "CREATE (x:Node {name:'x'})\n" +

        "CREATE\n" +
            "  (a)-[:REL {cost:2}]->(b),\n" +
            "  (b)-[:REL {cost:2}]->(c),\n" +
            "  (c)-[:REL {cost:2}]->(f),\n" +
            "  (f)-[:REL {cost:1}]->(e),\n" +
            "  (e)-[:REL {cost:1}]->(d),\n" +
            "  (d)-[:REL {cost:1}]->(a)\n";

    private Graph graph;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();

        runQuery(DB_CYPHER);

        graph = new GraphLoader(db)
                .withLabel("Node")
                .withRelationshipType("REL")
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .load(HugeGraphFactory.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        graph = null;
    }

    private long id(String name) {
        return runInTransaction(db, () -> db.findNode(Label.label("Node"), "name", name).getId());
    }

    private String name(long id) {
        String[] name = {""};
        runQuery(
            String.format("MATCH (n:Node) WHERE id(n)=%d RETURN n.name as name", id),
            row -> name[0] = row.getString("name")
        );
        return name[0];
    }

    @Test
    void testOutgoing() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = config(id("a"), id("f"), Direction.OUTGOING);
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(OUTGOING) = " + path);
        assertEquals("abcf", path.toString());
        assertEquals(6.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testIncoming() {
        DijkstraConfig config = config(id("a"), id("f"), Direction.INCOMING);
        StringBuilder path = new StringBuilder();
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(INCOMING) = " + path);
        assertEquals("adef", path.toString());
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testBoth() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = config(id("a"), id("f"), Direction.BOTH);
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute(id("a"), id("f"), Direction.BOTH);

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(BOTH) = " + path);
        assertEquals("adef", path.toString());
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testUnreachableOutgoing() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = config(id("a"), id("x"), Direction.OUTGOING);
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    @Test
    void testUnreachableIncoming() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = config(id("a"), id("x"), Direction.INCOMING);
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    @Test
    void testUnreachableBoth() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = config(id("a"), id("x"), Direction.BOTH);
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }

    private DijkstraConfig config(long startNode, long endNode, Direction direction) {
        return ImmutableDijkstraConfig
            .builder()
            .startNode(startNode)
            .endNode(endNode)
            .direction(direction)
            .build();
    }
}
