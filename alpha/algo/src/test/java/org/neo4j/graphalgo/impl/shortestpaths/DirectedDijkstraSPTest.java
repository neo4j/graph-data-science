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
package org.neo4j.graphalgo.impl.shortestpaths;

import com.carrotsearch.hppc.procedures.IntProcedure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphdb.Label;

import java.util.Optional;

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

    private GraphStore graphStore;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();

        runQuery(DB_CYPHER);

        graphStore = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node")
            .putRelationshipProjectionsWithIdentifier("REL_OUT", RelationshipProjection.of("REL", Orientation.NATURAL, Aggregation.NONE))
            .putRelationshipProjectionsWithIdentifier("REL_IN", RelationshipProjection.of("REL", Orientation.REVERSE, Aggregation.NONE))
            .putRelationshipProjectionsWithIdentifier("REL_BOTH", RelationshipProjection.of("REL", Orientation.UNDIRECTED, Aggregation.NONE))
            .addRelationshipProperty(PropertyMapping.of("cost", Double.MAX_VALUE))
            .build()
            .graphStore(HugeGraphFactory.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        graphStore = null;
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
        DijkstraConfig config = DijkstraConfig.of(id("a"), id("f"));
        Graph graph = graphStore.getGraphProjection("REL_OUT", Optional.of("cost"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals("abcf", path.toString());
        assertEquals(6.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testIncoming() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = DijkstraConfig.of(id("a"), id("f"));
        Graph graph = graphStore.getGraphProjection("REL_IN", Optional.of("cost"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals("adef", path.toString());
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testBoth() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = DijkstraConfig.of(id("a"), id("f"));
        Graph graph = graphStore.getGraphProjection("REL_BOTH", Optional.of("cost"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute(id("a"), id("f"));

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        System.out.println("path(BOTH) = " + path);
        assertEquals("adef", path.toString());
        assertEquals(3.0, dijkstra.getTotalCost(), 0.1);
        assertEquals(4, dijkstra.getPathLength());
    }

    @Test
    void testUnreachableOutgoing() {
        StringBuilder path = new StringBuilder();
        DijkstraConfig config = DijkstraConfig.of(id("a"), id("x"));
        Graph graph = graphStore.getGraphProjection("REL_OUT", Optional.of("cost"));
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
        DijkstraConfig config = DijkstraConfig.of(id("a"), id("x"));
        Graph graph = graphStore.getGraphProjection("REL_IN", Optional.of("cost"));
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
        DijkstraConfig config = DijkstraConfig.of(id("a"), id("x"));
        Graph graph = graphStore.getGraphProjection("REL_BOTH", Optional.of("cost"));
        ShortestPathDijkstra dijkstra = new ShortestPathDijkstra(graph, config);
        dijkstra.compute();

        dijkstra.getFinalPath().forEach((IntProcedure) n -> path.append(name(n)));
        assertEquals(0, path.length());
        assertEquals(0, dijkstra.getPathLength());
        assertEquals(ShortestPathDijkstra.NO_PATH_FOUND, dijkstra.getTotalCost(), 0.1);
    }
}
