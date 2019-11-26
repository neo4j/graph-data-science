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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestGraph;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithMultipleRelTypeSupportTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.MultipleRelTypesSupport;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.graphalgo.GraphHelper.assertOutProperties;
import static org.neo4j.graphalgo.GraphHelper.assertOutRelationships;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.graphalgo.TestSupport.toArguments;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.DEFAULT;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.MAX;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.MIN;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.NONE;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.SKIP;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.SUM;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.Iterables.asSet;

class GraphLoaderMultipleRelTypesAndPropertiesTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (n1:Node1 {prop1: 1})" +
        ", (n2:Node2 {prop2: 2})" +
        ", (n3:Node3 {prop3: 3})" +
        ", (n1)-[:REL1 {prop1: 1}]->(n2)" +
        ", (n1)-[:REL2 {prop2: 2}]->(n3)" +
        ", (n2)-[:REL1 {prop3: 3, weight: 42}]->(n3)" +
        ", (n2)-[:REL3 {prop4: 4, weight: 1337}]->(n3)";

    private GraphDatabaseAPI db;

    private long id1;
    private long id2;
    private long id3;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    private void initDatabase() {
        db.execute(DB_CYPHER);
        id1 = db.execute("MATCH (n:Node1) RETURN id(n) AS id").<Long>columnAs("id").next();
        id2 = db.execute("MATCH (n:Node2) RETURN id(n) AS id").<Long>columnAs("id").next();
        id3 = db.execute("MATCH (n:Node3) RETURN id(n) AS id").<Long>columnAs("id").next();
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    void testLoadDuplicateRelationships(Class<? extends GraphFactory> graphFactory) {
        initDatabase();

        Graph graph = TestGraphLoader.from(db)
            .withDeduplicationStrategy(DeduplicationStrategy.NONE)
            .buildGraph(graphFactory);

        Graph expected = TestGraph.Builder.fromGdl(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-->(n2)" +
            "(n1)-->(n3)" +
            "(n2)-->(n3)" +
            "(n2)-->(n3)"
        );

        TestSupport.assertGraphEquals(expected, graph);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    void testLoadDuplicateRelationshipsWithWeightsOnCypher(Class<? extends GraphFactory> graphFactory) {
        initDatabase();
        Graph graph = TestGraphLoader.from(db)
            .withRelProperties(PropertyMapping.of("weight", 1.0))
            .withDeduplicationStrategy(NONE)
            .buildGraph(graphFactory);

        Graph expected = TestGraph.Builder.fromGdl(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: 42.0d}]->(n3)" +
            "(n2)-[{weight: 1337.0d}]->(n3)"
        );

        TestSupport.assertGraphEquals(expected, graph);
    }

    static Stream<Arguments> deduplicateWithWeightsParams() {
        return TestSupport.crossArguments(toArguments(TestSupport::allTypesWithMultipleRelTypeSupport), () -> Stream.of(
            Arguments.of(SUM, 1379.0),
            Arguments.of(MAX, 1337.0),
            Arguments.of(MIN, 42.0)
        ));
    }

    @ParameterizedTest
    @MethodSource("deduplicateWithWeightsParams")
    void testLoadDuplicateRelationshipsWithWeightsAggregation(
        Class<? extends GraphFactory> graphFactory,
        DeduplicationStrategy deduplicationStrategy,
        double expectedWeight
    ) {
        initDatabase();

        Graph graph = TestGraphLoader.from(db)
            .withDeduplicationStrategy(deduplicationStrategy)
            .withRelProperties(PropertyMapping.of("weight", 1.0))
            .buildGraph(graphFactory);

        Graph expected = TestGraph.Builder.fromGdl(String.format(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: %fd}]->(n3)", expectedWeight));

        TestSupport.assertGraphEquals(expected, graph);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    void testLoadDuplicateRelationshipsWithWeightsAggregation(Class<? extends GraphFactory> graphFactory) {
        initDatabase();

        Graph graph = TestGraphLoader.from(db)
            .withDeduplicationStrategy(SKIP)
            .withRelProperties(PropertyMapping.of("weight", 1.0))
            .buildGraph(graphFactory);

        String expectedGraph =
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: %fd}]->(n3)";

        Graph expected1 = TestGraph.Builder.fromGdl(String.format(expectedGraph, 42.0));
        Graph expected2 = TestGraph.Builder.fromGdl(String.format(expectedGraph, 1337.0));
        TestSupport.assertGraphEquals(Arrays.asList(expected1, expected2), graph);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport>
    void testLoadMultipleRelationships(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        initDatabase();

        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipType("REL1 | REL2")
            .buildGraphs(graphFactory);

        assertEquals(2, graphs.availableRelationshipTypes().size());
        assertEquals(graphs.availableRelationshipTypes(), asSet(asList("REL1", "REL2")));

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph rel2Graph = graphs.getGraph("REL2");
        Graph unionGraph = graphs.getGraph("REL1 | REL2");

        assertOutRelationships(rel1Graph, id1, id2);
        assertOutRelationships(rel2Graph, id1, id3);
        assertOutRelationships(unionGraph, id1, id2, id3);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport>
    void testLoadMultipleRelationshipsWithWeights(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        initDatabase();

        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipType("REL1 | REL2")
            .withRelProperties(PropertyMapping.of("prop1", 42D))
            .buildGraphs(graphFactory);

        assertEquals(2, graphs.availableRelationshipTypes().size());
        assertEquals(graphs.availableRelationshipTypes(), asSet(asList("REL1", "REL2")));

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph rel2Graph = graphs.getGraph("REL2");
        Graph unionGraph = graphs.getGraph("REL1 | REL2");

        assertOutProperties(rel1Graph, id1, 1D);
        assertOutProperties(rel2Graph, id1, 42D);
        assertOutProperties(unionGraph, id1, 1D, 42D);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport> void multipleRelProperties(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        db.execute(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 43, p2: 1338, p3: 10}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339, p3: 10}]->(b)" +
            ", (b)-[:REL {p1: 45, p2: 1340, p3: 10}]->(c)" +
            ", (b)-[:REL {p1: 46, p2: 1341, p3: 10}]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphsByRelationshipType graph = graphLoader.withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", 1.0, DeduplicationStrategy.NONE),
                PropertyMapping.of("agg2", "p2", 2.0, DeduplicationStrategy.NONE),
                PropertyMapping.of("agg3", "p3", 2.0, DeduplicationStrategy.NONE)
            )
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph p1 = graph.getGraph("", Optional.of("agg1"));
        assertEquals(4L, p1.nodeCount());
        assertOutProperties(p1, 0, 42, 43, 44);
        assertOutProperties(p1, 1, 45, 46);
        assertOutProperties(p1, 2);
        assertOutProperties(p1, 3);

        Graph p2 = graph.getGraph("", Optional.of("agg2"));
        assertEquals(4L, p2.nodeCount());
        assertOutProperties(p2, 0, 1337, 1338, 1339);
        assertOutProperties(p2, 1, 1340, 1341);
        assertOutProperties(p2, 2);
        assertOutProperties(p2, 3);

        Graph p3 = graph.getGraph("", Optional.of("agg3"));
        assertEquals(4L, p3.nodeCount());
        assertOutProperties(p3, 0, 2, 10, 10);
        assertOutProperties(p3, 1, 10, 10);
        assertOutProperties(p3, 2);
        assertOutProperties(p3, 3);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport> void multipleRelPropertiesWithDefaultValues(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
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
        GraphsByRelationshipType graphs = graphLoader.withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", 1.0, MIN),
                PropertyMapping.of("agg2", "p1", 50.0, MAX),
                PropertyMapping.of("agg3", "p1", 3.0, SUM)
            )
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph p1 = graphs.getGraph("", Optional.of("agg1"));
        assertEquals(2L, p1.nodeCount());
        assertOutProperties(p1, 0, 1);
        assertOutProperties(p1, 1, 1);

        Graph p2 = graphs.getGraph("", Optional.of("agg2"));
        assertEquals(2L, p2.nodeCount());
        assertOutProperties(p2, 0, 51);
        assertOutProperties(p2, 1, 50);

        Graph p3 = graphs.getGraph("", Optional.of("agg3"));
        assertEquals(2L, p3.nodeCount());
        assertOutProperties(p3, 0, 93);
        assertOutProperties(p3, 1, 48);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport>
    void multipleRelPropertiesWithIncompatibleDeduplicationStrategies(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        db.execute(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339}]->(b)" +
            ", (b)-[:REL {p1: 45, p2: 1340}]->(c)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(d)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            graphLoader.withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(
                    PropertyMapping.of("p1", "p1", 1.0, DeduplicationStrategy.NONE),
                    PropertyMapping.of("p2", "p2", 2.0, SUM)
                )
                .withDirection(OUTGOING)
                .build(graphFactory)
                .importAllGraphs());

        assertThat(
            ex.getMessage(),
            containsString(
                "Conflicting relationship property deduplication strategies, it is not allowed to mix `NONE` with aggregations.")
        );
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport> void multipleAggregationsFromSameProperty(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
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
        GraphsByRelationshipType graph = graphLoader.withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", 1.0, MAX),
                PropertyMapping.of("agg2", "p1", 2.0, MIN)
            )
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph p1 = graph.getGraph("", Optional.of("agg1"));
        assertEquals(2L, p1.nodeCount());
        assertOutProperties(p1, 0, 44);
        assertOutProperties(p1, 1, 46);

        Graph p2 = graph.getGraph("", Optional.of("agg2"));
        assertEquals(2L, p2.nodeCount());
        assertOutProperties(p2, 0, 42);
        assertOutProperties(p2, 1, 45);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport> void multipleRelTypesWithSameProperty(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        db.execute(
            "CREATE" +
            "  (a:Node)" +
            ", (a)-[:REL_1 {p1: 43}]->(a)" +
            ", (a)-[:REL_1 {p1: 84}]->(a)" +
            ", (a)-[:REL_2 {p1: 42}]->(a)" +
            ", (a)-[:REL_3 {p1: 44}]->(a)");

        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphsByRelationshipType graph = graphLoader.withAnyLabel()
            .withRelationshipStatement("REL_1 | REL_2 | REL_3")
            .withDeduplicationStrategy(MAX)
            .withRelationshipProperties(
                PropertyMapping.of("agg", "p1", 1.0, MAX)
            )
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph g = graph.getGraph("", Optional.of("agg"));
        assertEquals(1L, g.nodeCount());
        assertEquals(3L, g.relationshipCount());
        assertOutProperties(g, 0, 42, 44, 84);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport> void multipleRelTypeGraphsCanBeReleased(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        initDatabase();
        GraphsByRelationshipType graphs = new GraphLoader(db)
            .withAnyLabel()
            .withRelationshipType("REL1 | REL2")
            .build(graphFactory)
            .importAllGraphs();

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph unionGraph = graphs.getUnion();

        rel1Graph.release();

        assertThrows(NullPointerException.class, () -> rel1Graph.forEachNode(n -> {
            rel1Graph.forEachRelationship(n, OUTGOING, (s, t) -> true);
            return true;
        }), "Graph should release");

        unionGraph.release();

        assertThrows(NullPointerException.class, () -> unionGraph.forEachNode(n -> {
            unionGraph.forEachRelationship(n, OUTGOING, (s, t) -> true);
            return true;
        }), "UnionGraph should release");
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport> void multipleRelTypeGraphsGiveCorrectElementCounts(Class<T> graphFactory) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        initDatabase();
        GraphsByRelationshipType graphs = new GraphLoader(db)
            .withAnyLabel()
            .withRelationshipType("REL1 | REL2 | REL3")
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph unionGraph = graphs.getUnion();

        long unionGraphExpectedNodeCount;
        long unionGraphExpectedRelCount;
        long rel1GraphExpectedNodeCount;
        long rel1GraphExpectedRelCount;
        try (Transaction tx = db.beginTx()) {
            unionGraphExpectedNodeCount = db.getAllNodes().stream().count();
            unionGraphExpectedRelCount = db.getAllRelationships().stream().count();
            // The graphs share the node mapping, so we expect the node count for a subgraph
            // to be equal to the node Count for the entire Neo4j graph
            rel1GraphExpectedNodeCount = db.getAllNodes().stream().count();
            rel1GraphExpectedRelCount = db
                .getAllRelationships()
                .stream()
                .filter(r -> r.isType(RelationshipType.withName("REL1")))
                .count();
        }

        assertEquals(unionGraphExpectedNodeCount, unionGraph.nodeCount());
        assertEquals(unionGraphExpectedRelCount, unionGraph.relationshipCount());
        assertEquals(rel1GraphExpectedNodeCount, rel1Graph.nodeCount());
        assertEquals(rel1GraphExpectedRelCount, rel1Graph.relationshipCount());
    }

    static Stream<Arguments> globalAndLocalDeduplicationArguments() {
        return Stream.of(
            Arguments.of(MAX, DEFAULT, DEFAULT, 44, 46, 1339, 1341),
            Arguments.of(MIN, DEFAULT, MAX, 42, 45, 1339, 1341),
            Arguments.of(DEFAULT, DEFAULT, DEFAULT, 42, 45, 1337, 1340),
            Arguments.of(DEFAULT, DEFAULT, SUM, 42, 45, 4014, 2681),
            Arguments.of(DEFAULT, MAX, SUM, 44, 46, 4014, 2681)
        );
    }

    static Stream<Arguments> graphImplWithGlobalAndLocalDeduplicationArguments() {
        return crossArguments(
            toArguments(TestSupport::allTypesWithMultipleRelTypeSupport),
            GraphLoaderMultipleRelTypesAndPropertiesTest::globalAndLocalDeduplicationArguments
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplWithGlobalAndLocalDeduplicationArguments")
    <T extends GraphFactory & MultipleRelTypesSupport>
    void multipleRelPropertiesWithGlobalAndLocalDeduplicationStrategy(
        Class<T> graphFactory,
        DeduplicationStrategy globalDeduplicationStrategy,
        DeduplicationStrategy localDeduplicationStrategy1,
        DeduplicationStrategy localDeduplicationStrategy2,
        double expectedNodeAP1,
        double expectedNodeBP1,
        double expectedNodeAP2,
        double expectedNodeBP2
    ) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        db.execute("" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                   " (a)-[:REL {p1: 43, p2: 1338}]->(a)," +
                   " (a)-[:REL {p1: 44, p2: 1339}]->(a)," +
                   " (b)-[:REL {p1: 45, p2: 1340}]->(b)," +
                   " (b)-[:REL {p1: 46, p2: 1341}]->(b)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);

        final GraphsByRelationshipType graph = graphLoader.withAnyLabel()
            .withAnyRelationshipType()
            .withDeduplicationStrategy(globalDeduplicationStrategy)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", 1.0, localDeduplicationStrategy1),
                PropertyMapping.of("p2", "p2", 2.0, localDeduplicationStrategy2)
            )
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph p1 = graph.getGraph("", Optional.of("p1"));
        assertEquals(4L, p1.nodeCount());
        assertOutProperties(p1, 0, expectedNodeAP1);
        assertOutProperties(p1, 1, expectedNodeBP1);

        Graph p2 = graph.getGraph("", Optional.of("p2"));
        assertEquals(4L, p2.nodeCount());
        assertOutProperties(p2, 0, expectedNodeAP2);
        assertOutProperties(p2, 1, expectedNodeBP2);
    }

    static Stream<Arguments> localDeduplicationArguments() {
        return Stream.of(
            Arguments.of(SKIP, 43, 45, 1338, 1340),
            Arguments.of(MIN, 42, 45, 1337, 1340),
            Arguments.of(MAX, 44, 46, 1339, 1341),
            Arguments.of(SUM, 129, 91, 4014, 2681)
        );
    }

    static Stream<Arguments> graphImplWithLocalDeduplicationArguments() {
        return crossArguments(
            toArguments(TestSupport::allTypesWithMultipleRelTypeSupport),
            GraphLoaderMultipleRelTypesAndPropertiesTest::localDeduplicationArguments
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplWithLocalDeduplicationArguments")
    <T extends GraphFactory & MultipleRelTypesSupport>
    void multipleRelPropertiesWithDeduplication(
        Class<T> graphFactory,
        DeduplicationStrategy deduplicationStrategy,
        double expectedNodeAP1,
        double expectedNodeBP1,
        double expectedNodeAP2,
        double expectedNodeBP2
    ) {
        assumeFalse(graphFactory.equals(CypherGraphFactory.class));
        db.execute(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339}]->(a)" +
            ", (b)-[:REL {p1: 45, p2: 1340}]->(b)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(b)");
        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT);
        GraphsByRelationshipType graph = graphLoader.withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", 1.0, deduplicationStrategy),
                PropertyMapping.of("p2", "p2", 2.0, deduplicationStrategy)
            )
            .withDirection(OUTGOING)
            .build(graphFactory)
            .importAllGraphs();

        Graph p1 = graph.getGraph("", Optional.of("p1"));
        assertEquals(2L, p1.nodeCount());
        assertOutProperties(p1, 0, expectedNodeAP1);
        assertOutProperties(p1, 1, expectedNodeBP1);

        Graph p2 = graph.getGraph("", Optional.of("p2"));
        assertEquals(2L, p2.nodeCount());
        assertOutProperties(p2, 0, expectedNodeAP2);
        assertOutProperties(p2, 1, expectedNodeBP2);
    }
}
