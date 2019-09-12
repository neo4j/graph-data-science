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

import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class GraphLoaderTest {

    private GraphDatabaseAPI db;

    @BeforeEach
    void setUp() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphTypesTest
    void outgoing(Class<? extends GraphFactory> graphImpl) {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement("MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        Graph graph = graphLoader
                .withDirection(Direction.OUTGOING)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        checkOutRelationships(graph, 0, 0, 1);
        checkOutRelationships(graph, 1, 1, 2, 3);
        checkOutRelationships(graph, 2);
        checkOutRelationships(graph, 3);
    }

    @AllGraphTypesTest
    void outgoingWithoutDeduplication(Class<? extends GraphFactory> graphImpl) {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement("MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        Graph graph = graphLoader
                .withDirection(Direction.OUTGOING)
                .withDeduplicateRelationshipsStrategy(DeduplicationStrategy.NONE)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        checkOutRelationships(graph, 0, 0, 1);
        checkOutRelationships(graph, 1, 1, 2, 3);
        checkOutRelationships(graph, 2);
        checkOutRelationships(graph, 3);
    }

    @AllGraphTypesTest
    void incoming(Class<? extends GraphFactory> graphImpl) {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement("MATCH (n)<--(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        Graph graph = graphLoader
                .withDirection(Direction.INCOMING)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        if (graphImpl == CypherGraphFactory.class) {
            checkOutRelationships(graph, 0, 0);
            checkOutRelationships(graph, 1, 0, 1);
            checkOutRelationships(graph, 2, 1);
            checkOutRelationships(graph, 3, 1);
        } else {
            checkInRelationships(graph, 0, 0);
            checkInRelationships(graph, 1, 0, 1);
            checkInRelationships(graph, 2, 1);
            checkInRelationships(graph, 3, 1);
        }
    }

    @AllGraphTypesTest
    void both(Class<? extends GraphFactory> graphImpl) {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement("MATCH (n)--(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        Graph graph = graphLoader
                .withDirection(Direction.BOTH)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());

        if (graphImpl == CypherGraphFactory.class) {
            checkOutRelationships(graph, 0, 0, 1);
            checkOutRelationships(graph, 1, 0, 1, 2, 3);
            checkOutRelationships(graph, 2, 1);
            checkOutRelationships(graph, 3, 1);
        } else {
            checkOutRelationships(graph, 0, 0, 1);
            checkInRelationships(graph, 0, 0);

            checkOutRelationships(graph, 1, 1, 2, 3);
            checkInRelationships(graph, 1, 0, 1);

            checkOutRelationships(graph, 2);
            checkInRelationships(graph, 2, 1);

            checkOutRelationships(graph, 3);
            checkInRelationships(graph, 3, 1);
        }
    }

    @AllGraphTypesTest
    void undirectedWithDeduplicatoin(Class<? extends GraphFactory> graphImpl) {
        assumeTrue(graphImpl != GraphViewFactory.class);
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement(
                            "MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target UNION ALL MATCH (n)<--(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        Graph graph = graphLoader
                .undirected()
                .withDeduplicateRelationshipsStrategy(DeduplicationStrategy.SKIP)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        checkOutRelationships(graph, 0, 0, 1);
        checkOutRelationships(graph, 1, 0, 1, 2, 3);
        checkOutRelationships(graph, 2, 1);
        checkOutRelationships(graph, 3, 1);
    }

    @AllGraphTypesTest
    void undirectedWithoutDeduplication(Class<? extends GraphFactory> graphImpl) {
        assumeTrue(graphImpl.isAssignableFrom(GraphViewFactory.class));
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL]->(a)," +
                   " (b)-[:REL]->(b)," +
                   " (a)-[:REL]->(b)," +
                   " (b)-[:REL]->(c)," +
                   " (b)-[:REL]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement(
                            "MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target UNION ALL MATCH (n)<--(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }

        Graph graph = graphLoader
                .undirected()
                .withDeduplicateRelationshipsStrategy(DeduplicationStrategy.NONE)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        checkOutRelationships(graph, 0, 0, 0, 1);
        checkOutRelationships(graph, 1, 0, 1, 1, 2, 3);
        checkOutRelationships(graph, 2, 1);
        checkOutRelationships(graph, 3, 1);
    }

    @AllGraphTypesTest
    void testLargerGraphWithDeletions(Class<? extends GraphFactory> graphImpl) {
        db.execute("FOREACH (x IN range(1, 4098) | CREATE (:Node {index:x}))");
        db.execute("MATCH (n) WHERE n.index IN [1, 2, 3] DELETE n");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n:Node) RETURN id(n) AS id")
                    .withRelationshipStatement("MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target");
        } else {
            graphLoader
                    .withLabel("Node")
                    .withAnyRelationshipType();
        }
        graphLoader.load(graphImpl);
    }

    @AllGraphTypesTest
    void testUndirectedNodeWithSelfReference(Class<? extends GraphFactory> graphImpl) {
        assumeTrue(graphImpl != GraphViewFactory.class);
        runUndirectedNodeWithSelfReference(graphImpl, "" +
                                                      "CREATE (a:Node),(b:Node) " +
                                                      "CREATE" +
                                                      " (a)-[:REL]->(a)," +
                                                      " (a)-[:REL]->(b)"
        );
    }

    @AllGraphTypesTest
    void testUndirectedNodeWithSelfReference2(Class<? extends GraphFactory> graphImpl) {
        assumeTrue(graphImpl != GraphViewFactory.class);
        runUndirectedNodeWithSelfReference(graphImpl, "" +
                                                      "CREATE (a:Node),(b:Node) " +
                                                      "CREATE" +
                                                      " (a)-[:REL]->(b)," +
                                                      " (a)-[:REL]->(a)"
        );
    }

    private void runUndirectedNodeWithSelfReference(Class<? extends GraphFactory> graphImpl, String cypher) {
        db.execute(cypher);
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement(
                            "MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target UNION ALL MATCH (n)<--(m) RETURN id(n) AS source, id(m) AS target")
                    .withDeduplicateRelationshipsStrategy(DeduplicationStrategy.SKIP);
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        final Graph graph = graphLoader
                .undirected()
                .load(graphImpl);

        assertEquals(2L, graph.nodeCount());
        checkOutRelationships(graph, 0, 0, 1);
        checkOutRelationships(graph, 1, 0);
    }

    @AllGraphTypesTest
    void testUndirectedNodesWithMultipleSelfReferences(Class<? extends GraphFactory> graphImpl) {
        assumeTrue(graphImpl != GraphViewFactory.class);
        runUndirectedNodesWithMultipleSelfReferences(graphImpl, "" +
                                                                "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                                                                "CREATE" +
                                                                " (a)-[:REL]->(a)," +
                                                                " (b)-[:REL]->(b)," +
                                                                " (a)-[:REL]->(b)," +
                                                                " (b)-[:REL]->(c)," +
                                                                " (b)-[:REL]->(d)"
        );
    }

    @AllGraphTypesTest
    void testUndirectedNodesWithMultipleSelfReferences2(Class<? extends GraphFactory> graphImpl) {
        assumeTrue(graphImpl != GraphViewFactory.class);
        runUndirectedNodesWithMultipleSelfReferences(graphImpl, "" +
                                                                "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                                                                "CREATE" +
                                                                " (a)-[:REL]->(b)," +
                                                                " (a)-[:REL]->(a)," +
                                                                " (b)-[:REL]->(c)," +
                                                                " (b)-[:REL]->(d)," +
                                                                " (b)-[:REL]->(b)"
        );
    }

    private void runUndirectedNodesWithMultipleSelfReferences(Class<? extends GraphFactory> graphImpl, String cypher) {
        db.execute(cypher);
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                    .withNodeStatement("MATCH (n) RETURN id(n) AS id")
                    .withRelationshipStatement(
                            "MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target UNION ALL MATCH (n)<--(m) RETURN id(n) AS source, id(m) AS target")
                    .withDeduplicateRelationshipsStrategy(DeduplicationStrategy.SKIP);
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        final Graph graph = graphLoader
                .undirected()
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        checkOutRelationships(graph, 0, 0, 1);
        checkOutRelationships(graph, 1, 0, 1, 2, 3);
        checkOutRelationships(graph, 2, 1);
        checkOutRelationships(graph, 3, 1);
    }

    private void checkOutRelationships(Graph graph, long node, long... expected) {
        LongArrayList idList = new LongArrayList();
        graph.forEachOutgoing(node, (s, t) -> {
            idList.add(t);
            return true;
        });
        final long[] ids = idList.toArray();
        Arrays.sort(ids);
        Arrays.sort(expected);
        assertArrayEquals(expected, ids);
    }

    private void checkInRelationships(Graph graph, long node, long... expected) {
        LongArrayList idList = new LongArrayList();
        graph.forEachIncoming(node, (s, t) -> {
            idList.add(t);
            return true;
        });
        final long[] ids = idList.toArray();
        Arrays.sort(ids);
        Arrays.sort(expected);
        assertArrayEquals(expected, ids);
    }
}
