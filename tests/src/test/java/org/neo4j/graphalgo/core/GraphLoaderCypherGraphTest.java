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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.IntArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

class GraphLoaderCypherGraphTest {

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

    @Test
    void both() {
        DB.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT);
        Graph graph = graphLoader.withLabel("MATCH (n) RETURN id(n) AS id")
                .withRelationshipType("MATCH (a)--(b) RETURN id(a) AS source, id(b) AS target")
                .withDirection(Direction.BOTH)
                .undirected()
                .load(CypherGraphFactory.class);

        assertEquals(4L, graph.nodeCount());
        checkRelationships(graph, 0, 0, 1);
        checkRelationships(graph, 1, 0, 1, 2, 3);
        checkRelationships(graph, 2, 1);
        checkRelationships(graph, 3, 1);
    }

    @Test
    void outgoing() {
        DB.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT);
        Graph graph = graphLoader.withLabel("MATCH (n) RETURN id(n) AS id")
                .withRelationshipType("MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target")
                .withDirection(Direction.OUTGOING)
                .load(CypherGraphFactory.class);

        assertEquals(4L, graph.nodeCount());
        checkRelationships(graph, 0, 0, 1);
        checkRelationships(graph, 1, 1, 2, 3);
        checkRelationships(graph, 2);
        checkRelationships(graph, 3);
    }

    @Test
    void incoming() {
        DB.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(DB, Pools.DEFAULT);
        Graph graph = graphLoader.withLabel("MATCH (n) RETURN id(n) AS id")
                .withRelationshipType("MATCH (a)<--(b) RETURN id(a) AS source, id(b) AS target")
                .withDirection(Direction.INCOMING)
                .load(CypherGraphFactory.class);

        assertEquals(4L, graph.nodeCount());
        checkRelationships(graph, 0, 0);
        checkRelationships(graph, 1, 0, 1);
        checkRelationships(graph, 2, 1);
        checkRelationships(graph, 3, 1);
    }


    private void checkRelationships(Graph graph, int node, int... expected) {
        IntArrayList idList = new IntArrayList();
        graph.forEachOutgoing(node, longToIntConsumer((s, t) -> {
            idList.add(t);
            return true;
        }));
        final int[] ids = idList.toArray();
        Arrays.sort(ids);
        Arrays.sort(expected);
        assertArrayEquals(expected, ids);
    }
}
