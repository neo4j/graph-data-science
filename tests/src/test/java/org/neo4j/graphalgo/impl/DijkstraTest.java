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

import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.impl.yens.Dijkstra;
import org.neo4j.graphalgo.impl.yens.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.utils.Converters.longToIntConsumer;

/**
 * Test for specialized dijkstra implementation with filters & maxDepth
 *
 *
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 *
 * @author mknblch
 */
class DijkstraTest {

    private GraphDatabaseAPI db;

    private static Graph graph;
    private static LongArrayList edgeBlackList;
    private static Dijkstra dijkstra;

    @BeforeEach
    void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE (g:Node {name:'g'})\n" +
                "CREATE" +
                " (a)-[:TYPE {cost:2.0}]->(b),\n" +
                " (a)-[:TYPE {cost:1.0}]->(c),\n" +
                " (b)-[:TYPE {cost:1.0}]->(d),\n" +
                " (c)-[:TYPE {cost:2.0}]->(d),\n" +
                " (d)-[:TYPE {cost:1.0}]->(e),\n" +
                " (d)-[:TYPE {cost:2.0}]->(f),\n" +
                " (e)-[:TYPE {cost:2.0}]->(g),\n" +
                " (f)-[:TYPE {cost:1.0}]->(g)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .undirected()
                .load(HugeGraphFactory.class);

        edgeBlackList = new LongArrayList();

        dijkstra = new Dijkstra(graph)
                .withDirection(Direction.OUTGOING)
                .withFilter(longToIntConsumer((s, t) -> !edgeBlackList.contains(RawValues.combineIntInt(s, t))));
    }

    private int id(String name) {
        Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return Math.toIntExact(graph.toMappedNodeId(node[0].getId()));
    }

    @Test
    void testNoFilter() {
        edgeBlackList.clear();
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        System.out.println("weightedPath = " + weightedPath);
    }

    @Test
    void testFilterACDF() {
        edgeBlackList.clear();
        edgeBlackList.add(RawValues.combineIntInt(id("a"), id("c")));
        edgeBlackList.add(RawValues.combineIntInt(id("d"), id("f")));
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        assertTrue(weightedPath.containsNode(id("b")));
        assertTrue(weightedPath.containsNode(id("e")));
    }

    @Test
    void testFilterABDE() {
        edgeBlackList.clear();
        edgeBlackList.add(RawValues.combineIntInt(id("a"), id("b")));
        edgeBlackList.add(RawValues.combineIntInt(id("d"), id("e")));
        final WeightedPath weightedPath = dijkstra();
        assertEquals(5, weightedPath.size());
        assertTrue(weightedPath.containsNode(id("c")));
        assertTrue(weightedPath.containsNode(id("f")));
    }

    @Test
    void testMaxDepth() {
        assertTrue(dijkstra.compute(id("a"), id("d"), 4).isPresent());
        assertFalse(dijkstra.compute(id("a"), id("d"), 3).isPresent());
    }

    private WeightedPath dijkstra() {
        return dijkstra.compute(id("a"), id("g"))
                .orElseThrow(() -> new AssertionError("No path"));
    }

}
