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
package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

final class LoadingTest {

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraphDb() {
        DB = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void clearDb() {
        DB.execute("MATCH (n) DETACH DELETE n");
    }

    @AfterAll
    static void shutdownGraphDb() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testBasicLoading(Class<? extends GraphFactory> graphFactory) {
        assumeFalse(graphFactory.isAssignableFrom(GraphViewFactory.class));
        DB.execute("CREATE (a:Node {name:'a'})\n" +
                   "CREATE (b:Node {name:'b'})\n" +
                   "CREATE (c:Node {name:'c'})\n" +
                   "CREATE (d:Node2 {name:'d'})\n" +
                   "CREATE (e:Node2 {name:'e'})\n" +

                   "CREATE" +
                   " (a)-[:TYPE {prop:1}]->(b),\n" +
                   " (e)-[:TYPE {prop:2}]->(d),\n" +
                   " (d)-[:TYPE {prop:3}]->(c),\n" +
                   " (a)-[:TYPE {prop:4}]->(c),\n" +
                   " (a)-[:TYPE {prop:5}]->(d),\n" +
                   " (a)-[:TYPE2 {prop:6}]->(d),\n" +
                   " (b)-[:TYPE2 {prop:7}]->(e),\n" +
                   " (a)-[:TYPE2 {prop:8}]->(e)");

        final Graph graph = new GraphLoader(DB)
                .withDirection(Direction.OUTGOING)
                .withExecutorService(Pools.DEFAULT)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .load(graphFactory);

        assertEquals(3, graph.nodeCount());

        assertEquals(2, graph.degree(0, Direction.OUTGOING));
        assertEquals(0, graph.degree(1, Direction.OUTGOING));
        assertEquals(0, graph.degree(2, Direction.OUTGOING));

        checkRels(graph, 0, 1, 2);
        checkRels(graph, 1);
        checkRels(graph, 2);
    }

    private void checkRels(Graph graph, int node, long... expected) {
        Arrays.sort(expected);
        assertArrayEquals(expected, mkRels(graph, node));
    }

    private long[] mkRels(Graph graph, int node) {
        final LongArrayList rels = new LongArrayList();
        graph.forEachOutgoing(node, (s, t) -> {
            rels.add(t);
            return true;
        });
        long[] ids = rels.toArray();
        Arrays.sort(ids);
        return ids;
    }
}
