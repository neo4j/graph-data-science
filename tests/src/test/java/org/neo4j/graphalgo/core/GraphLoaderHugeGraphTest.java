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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.huge.loader.CypherGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphByType;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.graphalgo.GraphHelper.assertInRelationships;
import static org.neo4j.graphalgo.GraphHelper.assertOutRelationships;
import static org.neo4j.graphalgo.GraphHelper.assertOutWeights;

class GraphLoaderHugeGraphTest {

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
        assertOutRelationships(graph, 0, 0, 1);
        assertOutRelationships(graph, 1, 1, 2, 3);
        assertOutRelationships(graph, 2);
        assertOutRelationships(graph, 3);
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
                .withDeduplicationStrategy(DeduplicationStrategy.NONE)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        assertOutRelationships(graph, 0, 0, 1);
        assertOutRelationships(graph, 1, 1, 2, 3);
        assertOutRelationships(graph, 2);
        assertOutRelationships(graph, 3);
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
            assertOutRelationships(graph, 0, 0);
            assertOutRelationships(graph, 1, 0, 1);
            assertOutRelationships(graph, 2, 1);
            assertOutRelationships(graph, 3, 1);
        } else {
            assertInRelationships(graph, 0, 0);
            assertInRelationships(graph, 1, 0, 1);
            assertInRelationships(graph, 2, 1);
            assertInRelationships(graph, 3, 1);
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
            assertOutRelationships(graph, 0, 0, 1);
            assertOutRelationships(graph, 1, 0, 1, 2, 3);
            assertOutRelationships(graph, 2, 1);
            assertOutRelationships(graph, 3, 1);
        } else {
            assertOutRelationships(graph, 0, 0, 1);
            assertInRelationships(graph, 0, 0);

            assertOutRelationships(graph, 1, 1, 2, 3);
            assertInRelationships(graph, 1, 0, 1);

            assertOutRelationships(graph, 2);
            assertInRelationships(graph, 2, 1);

            assertOutRelationships(graph, 3);
            assertInRelationships(graph, 3, 1);
        }
    }

    @AllGraphTypesTest
    void undirectedWithDeduplication(Class<? extends GraphFactory> graphImpl) {
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
                .withDeduplicationStrategy(DeduplicationStrategy.SKIP)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        assertOutRelationships(graph, 0, 0, 1);
        assertOutRelationships(graph, 1, 0, 1, 2, 3);
        assertOutRelationships(graph, 2, 1);
        assertOutRelationships(graph, 3, 1);
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
                .withDeduplicationStrategy(DeduplicationStrategy.NONE)
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        assertOutRelationships(graph, 0, 0, 0, 1);
        assertOutRelationships(graph, 1, 0, 1, 1, 2, 3);
        assertOutRelationships(graph, 2, 1);
        assertOutRelationships(graph, 3, 1);
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
                    .withDeduplicationStrategy(DeduplicationStrategy.SKIP);
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        final Graph graph = graphLoader
                .undirected()
                .load(graphImpl);

        assertEquals(2L, graph.nodeCount());
        assertOutRelationships(graph, 0, 0, 1);
        assertOutRelationships(graph, 1, 0);
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
                    .withDeduplicationStrategy(DeduplicationStrategy.SKIP);
        } else {
            graphLoader
                    .withAnyLabel()
                    .withAnyRelationshipType();
        }
        final Graph graph = graphLoader
                .undirected()
                .load(graphImpl);

        assertEquals(4L, graph.nodeCount());
        assertOutRelationships(graph, 0, 0, 1);
        assertOutRelationships(graph, 1, 0, 1, 2, 3);
        assertOutRelationships(graph, 2, 1);
        assertOutRelationships(graph, 3, 1);
    }

    @Test
    void multipleRelProperties() {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                   " (a)-[:REL {p1: 43, p2: 1338, p3: 10}]->(a)," +
                   " (a)-[:REL {p1: 44, p2: 1339, p3: 10}]->(b)," +
                   " (b)-[:REL {p1: 45, p2: 1340, p3: 10}]->(c)," +
                   " (b)-[:REL {p1: 46, p2: 1341, p3: 10}]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphByType graph = graphLoader.withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(
                        PropertyMapping.of("agg1", "p1", 1.0, DeduplicationStrategy.NONE),
                        PropertyMapping.of("agg2", "p2", 2.0, DeduplicationStrategy.NONE),
                        PropertyMapping.of("agg3", "p3", 2.0, DeduplicationStrategy.NONE)
                )
                .withDirection(Direction.OUTGOING)
                .build(HugeGraphFactory.class)
                .loadGraphs();

        Graph p1 = graph.loadGraph("", Optional.of("agg1"));
        assertEquals(4L, p1.nodeCount());
        assertOutWeights(p1, 0, 42, 43, 44);
        assertOutWeights(p1, 1, 45, 46);
        assertOutWeights(p1, 2);
        assertOutWeights(p1, 3);

        Graph p2 = graph.loadGraph("", Optional.of("agg2"));
        assertEquals(4L, p2.nodeCount());
        assertOutWeights(p2, 0, 1337, 1338, 1339);
        assertOutWeights(p2, 1, 1340, 1341);
        assertOutWeights(p2, 2);
        assertOutWeights(p2, 3);

        Graph p3 = graph.loadGraph("", Optional.of("agg3"));
        assertEquals(4L, p3.nodeCount());
        assertOutWeights(p3, 0, 2, 10, 10);
        assertOutWeights(p3, 1, 10, 10);
        assertOutWeights(p3, 2);
        assertOutWeights(p3, 3);
    }

    @Test
    void multipleRelPropertiesWithDefaultValues() {
        db.execute(
                "CREATE" +
                "  (a:Node)" +
                ", (b:Node)" +
                ", (a)-[:REL]->(a)" +
                ", (a)-[:REL {p1: 39}]->(a)" +
                ", (a)-[:REL {p1: 51}]->(a)" +
                ", (b)-[:REL {p1: 45}]->(b)" +
                ", (b)-[:REL]->(b)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphByType graph = graphLoader.withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(
                        PropertyMapping.of("agg1", "p1", 1.0, DeduplicationStrategy.MIN),
                        PropertyMapping.of("agg2", "p1", 50.0, DeduplicationStrategy.MAX),
                        PropertyMapping.of("agg3", "p1", 3.0, DeduplicationStrategy.SUM)
                )
                .withDirection(Direction.OUTGOING)
                .build(HugeGraphFactory.class)
                .loadGraphs();

        Graph p1 = graph.loadGraph("", Optional.of("agg1"));
        assertEquals(2L, p1.nodeCount());
        assertOutWeights(p1, 0, 1);
        assertOutWeights(p1, 1, 1);

        Graph p2 = graph.loadGraph("", Optional.of("agg2"));
        assertEquals(2L, p2.nodeCount());
        assertOutWeights(p2, 0, 51);
        assertOutWeights(p2, 1, 50);

        Graph p3 = graph.loadGraph("", Optional.of("agg3"));
        assertEquals(2L, p3.nodeCount());
        assertOutWeights(p3, 0, 93);
        assertOutWeights(p3, 1, 48);
    }

    @Test
    void multipleRelPropertiesWithIncompatibleDeduplicationStrategies() {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                   " (a)-[:REL {p1: 43, p2: 1338}]->(a)," +
                   " (a)-[:REL {p1: 44, p2: 1339}]->(b)," +
                   " (b)-[:REL {p1: 45, p2: 1340}]->(c)," +
                   " (b)-[:REL {p1: 46, p2: 1341}]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                graphLoader.withAnyLabel()
                        .withAnyRelationshipType()
                        .withRelationshipProperties(
                                PropertyMapping.of("p1", "p1", 1.0, DeduplicationStrategy.NONE),
                                PropertyMapping.of("p2", "p2", 2.0, DeduplicationStrategy.SUM)
                        )
                        .withDirection(Direction.OUTGOING)
                        .build(HugeGraphFactory.class)
                        .loadGraphs());

        assertThat(
                ex.getMessage(),
                containsString(
                        "Conflicting relationship property deduplication strategies, it is not allowed to mix `NONE` with aggregations."));
    }

    @ParameterizedTest
    @CsvSource({
            "MAX,       DEFAULT, DEFAULT,   44, 46, 1339, 1341",
            "MIN,       DEFAULT, MAX,       42, 45, 1339, 1341",
            "DEFAULT,   DEFAULT, DEFAULT,   42, 45, 1337, 1340",
            "DEFAULT,   DEFAULT, SUM,       42, 45, 4014, 2681",
            "DEFAULT,   MAX,     SUM,       44, 46, 4014, 2681"
    })
    void multipleRelPropertiesWithGlobalAndLocalDeduplicationStrategy(
            DeduplicationStrategy globalDeduplicationStrategy,
            DeduplicationStrategy localDeduplicationStrategy1,
            DeduplicationStrategy localDeduplicationStrategy2,
            double expectedNodeAP1,
            double expectedNodeBP1,
            double expectedNodeAP2,
            double expectedNodeBP2
    ) {
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                   " (a)-[:REL {p1: 43, p2: 1338}]->(a)," +
                   " (a)-[:REL {p1: 44, p2: 1339}]->(a)," +
                   " (b)-[:REL {p1: 45, p2: 1340}]->(b)," +
                   " (b)-[:REL {p1: 46, p2: 1341}]->(b)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);

        final GraphByType graph = graphLoader.withAnyLabel()
                .withAnyRelationshipType()
                .withDeduplicationStrategy(globalDeduplicationStrategy)
                .withRelationshipProperties(
                        PropertyMapping.of("p1", "p1", 1.0, localDeduplicationStrategy1),
                        PropertyMapping.of("p2", "p2", 2.0, localDeduplicationStrategy2)
                )
                .withDirection(Direction.OUTGOING)
                .build(HugeGraphFactory.class)
                .loadGraphs();

        Graph p1 = graph.loadGraph("", Optional.of("p1"));
        assertEquals(4L, p1.nodeCount());
        assertOutWeights(p1, 0, expectedNodeAP1);
        assertOutWeights(p1, 1, expectedNodeBP1);

        Graph p2 = graph.loadGraph("", Optional.of("p2"));
        assertEquals(4L, p2.nodeCount());
        assertOutWeights(p2, 0, expectedNodeAP2);
        assertOutWeights(p2, 1, expectedNodeBP2);
    }

    @ParameterizedTest
    @CsvSource({
            "SKIP, 43, 45, 1338, 1340",
            "MIN, 42, 45, 1337, 1340",
            "MAX, 44, 46, 1339, 1341",
            "SUM, 129, 91, 4014, 2681",
    })
    void multipleRelPropertiesWithDeduplication(
            DeduplicationStrategy deduplicationStrategy,
            double expectedNodeAP1,
            double expectedNodeBP1,
            double expectedNodeAP2,
            double expectedNodeBP2) {
        db.execute("" +
                   "CREATE (a:Node),(b:Node) " +
                   "CREATE" +
                   " (a)-[:REL {p1: 43, p2: 1338}]->(a)," +
                   " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                   " (a)-[:REL {p1: 44, p2: 1339}]->(a)," +
                   " (b)-[:REL {p1: 45, p2: 1340}]->(b)," +
                   " (b)-[:REL {p1: 46, p2: 1341}]->(b)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphByType graph = graphLoader.withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(
                        PropertyMapping.of("p1", "p1", 1.0, deduplicationStrategy),
                        PropertyMapping.of("p2", "p2", 2.0, deduplicationStrategy)
                )
                .withDirection(Direction.OUTGOING)
                .build(HugeGraphFactory.class)
                .loadGraphs();

        Graph p1 = graph.loadGraph("", Optional.of("p1"));
        assertEquals(2L, p1.nodeCount());
        assertOutWeights(p1, 0, expectedNodeAP1);
        assertOutWeights(p1, 1, expectedNodeBP1);

        Graph p2 = graph.loadGraph("", Optional.of("p2"));
        assertEquals(2L, p2.nodeCount());
        assertOutWeights(p2, 0, expectedNodeAP2);
        assertOutWeights(p2, 1, expectedNodeBP2);
    }

    @Test
    void multipleAggregationsFromSameProperty() {
        db.execute(
                   "CREATE" +
                   "  (a:Node)" +
                   ", (b:Node)" +
                   ", (a)-[:REL {p1: 43}]->(a)" +
                   ", (a)-[:REL {p1: 42}]->(a)" +
                   ", (a)-[:REL {p1: 44}]->(a)" +
                   ", (b)-[:REL {p1: 45}]->(b)" +
                   ", (b)-[:REL {p1: 46}]->(b)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphByType graph = graphLoader.withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(
                        PropertyMapping.of("agg1", "p1", 1.0, DeduplicationStrategy.MAX),
                        PropertyMapping.of("agg2", "p1", 2.0, DeduplicationStrategy.MIN)
                )
                .withDirection(Direction.OUTGOING)
                .build(HugeGraphFactory.class)
                .loadGraphs();

        Graph p1 = graph.loadGraph("", Optional.of("agg1"));
        assertEquals(2L, p1.nodeCount());
        assertOutWeights(p1, 0, 44);
        assertOutWeights(p1, 1, 46);

        Graph p2 = graph.loadGraph("", Optional.of("agg2"));
        assertEquals(2L, p2.nodeCount());
        assertOutWeights(p2, 0, 42);
        assertOutWeights(p2, 1, 45);
    }

    @Test
    void multipleRelTypesWithSameProperty() {
        db.execute(
                "CREATE" +
                "  (a:Node)" +
                ", (a)-[:REL_1 {p1: 43}]->(a)" +
                ", (a)-[:REL_1 {p1: 84}]->(a)" +
                ", (a)-[:REL_2 {p1: 42}]->(a)" +
                ", (a)-[:REL_3 {p1: 44}]->(a)");

        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphByType graph = graphLoader.withAnyLabel()
                .withRelationshipStatement("REL_1 | REL_2 | REL_3")
                .withDeduplicationStrategy(DeduplicationStrategy.MAX)
                .withRelationshipProperties(
                        PropertyMapping.of("agg", "p1", 1.0, DeduplicationStrategy.MAX)
                )
                .withDirection(Direction.OUTGOING)
                .build(HugeGraphFactory.class)
                .loadGraphs();

        Graph g = graph.loadGraph("", Optional.of("agg"));
        assertEquals(1L, g.nodeCount());
        assertEquals(3L, g.relationshipCount());
        assertOutWeights(g, 0, 42, 44, 84);
    }
}
