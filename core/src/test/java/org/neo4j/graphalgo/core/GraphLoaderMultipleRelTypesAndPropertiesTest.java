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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.graphalgo.TestSupport.toArguments;
import static org.neo4j.graphalgo.core.Aggregation.DEFAULT;
import static org.neo4j.graphalgo.core.Aggregation.MAX;
import static org.neo4j.graphalgo.core.Aggregation.MIN;
import static org.neo4j.graphalgo.core.Aggregation.NONE;
import static org.neo4j.graphalgo.core.Aggregation.SINGLE;
import static org.neo4j.graphalgo.core.Aggregation.SUM;
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

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphTypesTest
    void parallelRelationshipsWithoutProperties(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db)
            .withDefaultAggregation(Aggregation.NONE)
            .buildGraph(graphFactory);

        Graph expected = fromGdl(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-->(n2)" +
            "(n1)-->(n3)" +
            "(n2)-->(n3)" +
            "(n2)-->(n3)"
        );

        assertGraphEquals(expected, graph);
    }

    @AllGraphTypesTest
    void parallelRelationships(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .withDefaultAggregation(NONE)
            .buildGraph(graphFactory);

        Graph expected = fromGdl(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: 42.0d}]->(n3)" +
            "(n2)-[{weight: 1337.0d}]->(n3)"
        );

        assertGraphEquals(expected, graph);
    }

    static Stream<Arguments> deduplicateWithWeightsParams() {
        return crossArguments(toArguments(TestSupport::allTypes), () -> Stream.of(
            Arguments.of(SUM, 1379.0),
            Arguments.of(MAX, 1337.0),
            Arguments.of(MIN, 42.0)
        ));
    }

    @ParameterizedTest
    @MethodSource("deduplicateWithWeightsParams")
    void parallelRelationshipsWithAggregation(
        Class<? extends GraphFactory> graphFactory,
        Aggregation aggregation,
        double expectedWeight
    ) {
        Graph graph = TestGraphLoader.from(db)
            .withDefaultAggregation(aggregation)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .buildGraph(graphFactory);

        Graph expected = fromGdl(String.format(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: %fd}]->(n3)", expectedWeight
        ));

        assertGraphEquals(expected, graph);
    }

    @AllGraphTypesTest
    void parallelRelationshipsWithAggregation_SINGLE(Class<? extends GraphFactory> graphFactory) {
        Graph graph = TestGraphLoader.from(db)
            .withDefaultAggregation(SINGLE)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .buildGraph(graphFactory);

        String expectedGraph =
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: %fd}]->(n3)";

        Graph expected1 = fromGdl(String.format(expectedGraph, 42.0));
        Graph expected2 = fromGdl(String.format(expectedGraph, 1337.0));
        assertGraphEquals(Arrays.asList(expected1, expected2), graph);
    }

    @AllGraphTypesTest
    void multipleTypes(Class<? extends GraphFactory> graphFactory) {
        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipType("REL1 | REL2")
            .buildGraphs(graphFactory);

        assertEquals(2, graphs.availableRelationshipTypes().size());
        assertEquals(graphs.availableRelationshipTypes(), asSet(asList("REL1", "REL2")));

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph rel2Graph = graphs.getGraph("REL2");
        Graph unionGraph = graphs.getGraph("REL1 | REL2");

        assertGraphEquals(fromGdl("(a)-->(b)-->(c)"), rel1Graph);
        assertGraphEquals(fromGdl("(a)-->(c), (b)"), rel2Graph);
        assertGraphEquals(fromGdl("(a)-->(b)-->(c)<--(a)"), unionGraph);
    }

    @AllGraphTypesTest
    void multipleTypesWithProperties(Class<? extends GraphFactory> graphFactory) {
        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipType("REL1 | REL2")
            .withRelationshipProperties(PropertyMapping.of("prop1", 1337D))
            .buildGraphs(graphFactory);

        assertEquals(2, graphs.availableRelationshipTypes().size());
        assertEquals(graphs.availableRelationshipTypes(), asSet(asList("REL1", "REL2")));

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph rel2Graph = graphs.getGraph("REL2");
        Graph unionGraph = graphs.getGraph("REL1 | REL2");

        assertGraphEquals(fromGdl("(a)-[]->(b)-[]->(c)"), rel1Graph);
        assertGraphEquals(fromGdl("(a)-[]->(c), (b)"), rel2Graph);
        assertGraphEquals(fromGdl("(a)-[]->(b)-[]->(c)<-[]-(a)"), unionGraph);
    }

    @AllGraphTypesTest
    void multipleProperties(Class<? extends GraphFactory> graphFactory) {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        runQuery(localDb,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 43, p2: 1338, p3: 10}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339, p3: 10}]->(b)" +
            ", (b)-[:REL {p1: 45, p2: 1340, p3: 10}]->(c)" +
            ", (b)-[:REL {p1: 46, p2: 1341, p3: 10}]->(d)"
        );

        GraphsByRelationshipType graphs = TestGraphLoader.from(localDb)
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", 1.0, Aggregation.NONE),
                PropertyMapping.of("agg2", "p2", 2.0, Aggregation.NONE),
                PropertyMapping.of("agg3", "p3", 2.0, Aggregation.NONE)
            )
            .buildGraphs(graphFactory);

        Graph p1Graph = graphs.getGraph("", Optional.of("agg1"));
        Graph expectedP1Graph = fromGdl(
            "(a)-[{w: 42}]->(a)" +
            "(a)-[{w: 43}]->(a)" +
            "(a)-[{w: 44}]->(b)" +
            "(b)-[{w: 45}]->(c)" +
            "(b)-[{w: 46}]->(d)"
        );
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph("", Optional.of("agg2"));
        Graph expectedP2Graph = fromGdl(
            "(a)-[{w: 1337}]->(a)" +
            "(a)-[{w: 1338}]->(a)" +
            "(a)-[{w: 1339}]->(b)" +
            "(b)-[{w: 1340}]->(c)" +
            "(b)-[{w: 1341}]->(d)"
        );
        assertGraphEquals(expectedP2Graph, p2Graph);

        Graph p3Graph = graphs.getGraph("", Optional.of("agg3"));
        Graph expectedP3Graph = fromGdl(
            "(a)-[{w: 2}]->(a)" +
            "(a)-[{w: 10}]->(a)" +
            "(a)-[{w: 10}]->(b)" +
            "(b)-[{w: 10}]->(c)" +
            "(b)-[{w: 10}]->(d)"
        );
        assertGraphEquals(expectedP3Graph, p3Graph);
    }

    @AllGraphTypesTest
    void multiplePropertiesWithDefaultValues(Class<? extends GraphFactory> graphFactory) {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        runQuery(localDb,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (a)-[:REL]->(a)" +
            ", (a)-[:REL {p1: 39}]->(a)" +
            ", (a)-[:REL {p1: 51}]->(a)" +
            ", (b)-[:REL {p1: 45}]->(b)" +
            ", (b)-[:REL]->(b)"
        );
        GraphsByRelationshipType graphs = TestGraphLoader.from(localDb)
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", 1.0, MIN),
                PropertyMapping.of("agg2", "p1", 50.0, MAX),
                PropertyMapping.of("agg3", "p1", 3.0, SUM)
            )
            .buildGraphs(graphFactory);

        Graph p1Graph = graphs.getGraph("", Optional.of("agg1"));
        Graph expectedP1Graph = fromGdl(
            "(a)-[{w: 1.0d}]->(a)" +
            "(b)-[{w: 1.0d}]->(b)"
        );
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph("", Optional.of("agg2"));
        Graph expectedP2Graph = fromGdl(
            "(a)-[{w: 51.0d}]->(a)" +
            "(b)-[{w: 50.0d}]->(b)"
        );
        assertGraphEquals(expectedP2Graph, p2Graph);

        Graph p3Graph = graphs.getGraph("", Optional.of("agg3"));
        Graph expectedP3Graph = fromGdl(
            "(a)-[{w: 93.0d}]->(a)" +
            "(b)-[{w: 48.0d}]->(b)"
        );
        assertGraphEquals(expectedP3Graph, p3Graph);
    }

    @AllGraphTypesTest
    void multiplePropertiesWithIncompatibleAggregations(Class<? extends GraphFactory> graphFactory) {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        runQuery(localDb,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339}]->(b)" +
            ", (b)-[:REL {p1: 45, p2: 1340}]->(c)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(d)"
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            TestGraphLoader.from(localDb)
                .withRelationshipProperties(
                    PropertyMapping.of("p1", "p1", 1.0, Aggregation.NONE),
                    PropertyMapping.of("p2", "p2", 2.0, SUM)
                )
                .buildGraphs(graphFactory)
        );

        assertThat(
            ex.getMessage(),
            containsString(
                "Conflicting relationship property aggregations, it is not allowed to mix `NONE` with aggregations.")
        );
    }

    @AllGraphTypesTest
    void singlePropertyWithAggregations(Class<? extends GraphFactory> graphFactory) {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        runQuery(localDb,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (a)-[:REL {p1: 43}]->(a)" +
            ", (a)-[:REL {p1: 42}]->(a)" +
            ", (a)-[:REL {p1: 44}]->(a)" +
            ", (b)-[:REL {p1: 45}]->(b)" +
            ", (b)-[:REL {p1: 46}]->(b)"
        );

        GraphsByRelationshipType graphs = TestGraphLoader.from(localDb)
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", 1.0, MAX),
                PropertyMapping.of("agg2", "p1", 2.0, MIN)
            )
            .buildGraphs(graphFactory);

        Graph p1Graph = graphs.getGraph("", Optional.of("agg1"));
        Graph expectedP1Graph = fromGdl(
            "(a)-[{w: 44.0d}]->(a)" +
            "(b)-[{w: 46.0d}]->(b)"
        );
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph("", Optional.of("agg2"));
        Graph expectedP2Graph = fromGdl(
            "(a)-[{w: 42.0d}]->(a)" +
            "(b)-[{w: 45.0d}]->(b)"
        );
        assertGraphEquals(expectedP2Graph, p2Graph);
    }

    static Stream<Arguments> globalAndLocalAggregationsArguments() {
        return Stream.of(
            Arguments.of(MAX, DEFAULT, DEFAULT, 44, 46, 1339, 1341),
            Arguments.of(MIN, DEFAULT, MAX, 42, 45, 1339, 1341),
            Arguments.of(MIN, MAX, SUM, 44, 46, 4014, 2681)
        );
    }

    static Stream<Arguments> localAggregationArguments() {
        return Stream.of(
            Arguments.of(MIN, 42, 45, 1337, 1340),
            Arguments.of(MAX, 44, 46, 1339, 1341),
            Arguments.of(SUM, 129, 91, 4014, 2681)
        );
    }

    static Stream<Arguments> graphImplWithGlobalAndLocalAggregationArguments() {
        return crossArguments(
            toArguments(TestSupport::allTypes),
            GraphLoaderMultipleRelTypesAndPropertiesTest::globalAndLocalAggregationsArguments
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplWithGlobalAndLocalAggregationArguments")
    void multiplePropertiesWithGlobalAndLocalAggregations(
        Class<? extends GraphFactory> graphFactory,
        Aggregation globalAggregation,
        Aggregation localAggregation1,
        Aggregation localAggregation2,
        double expectedNodeAP1,
        double expectedNodeBP1,
        double expectedNodeAP2,
        double expectedNodeBP2
    ) {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, "" +
                   "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                   "CREATE" +
                   " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                   " (a)-[:REL {p1: 43, p2: 1338}]->(a)," +
                   " (a)-[:REL {p1: 44, p2: 1339}]->(a)," +
                   " (b)-[:REL {p1: 45, p2: 1340}]->(b)," +
                   " (b)-[:REL {p1: 46, p2: 1341}]->(b)");

        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withDefaultAggregation(globalAggregation)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", 1.0, localAggregation1),
                PropertyMapping.of("p2", "p2", 2.0, localAggregation2)
            )
            .buildGraphs(graphFactory);

        Graph p1Graph = graphs.getGraph("", Optional.of("p1"));
        Graph expectedP1Graph = fromGdl(String.format(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)" +
            "(c), (d)",
            expectedNodeAP1,
            expectedNodeBP1
        ));
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph("", Optional.of("p2"));
        Graph expectedP2Graph = fromGdl(String.format(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)" +
            "(c), (d)",
            expectedNodeAP2,
            expectedNodeBP2
        ));
        assertGraphEquals(expectedP2Graph, p2Graph);
    }

    @AllGraphTypesTest
    void multipleTypesWithSameProperty(Class<? extends GraphFactory> graphFactory) {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        runQuery(localDb,
            "CREATE" +
            "  (a:Node)" +
            ", (a)-[:REL_1 {p1: 43}]->(a)" +
            ", (a)-[:REL_1 {p1: 84}]->(a)" +
            ", (a)-[:REL_2 {p1: 42}]->(a)" +
            ", (a)-[:REL_3 {p1: 44}]->(a)");

        GraphsByRelationshipType graphs = TestGraphLoader.from(localDb)
            .withRelationshipType("REL_1 | REL_2 | REL_3")
            .withDefaultAggregation(MAX)
            .withRelationshipProperties(
                PropertyMapping.of("agg", "p1", 1.0, MAX)
            )
            .buildGraphs(graphFactory);

        Graph graph = graphs.getGraph("", Optional.of("agg"));
        assertEquals(3L, graph.relationshipCount());
        Graph expectedGraph = fromGdl(
            "(a)-[{w: 42.0d}]->(a)" +
            "(a)-[{w: 44.0d}]->(a)" +
            "(a)-[{w: 84.0d}]->(a)"
        );
        assertGraphEquals(expectedGraph, graph);
    }

    static Stream<Arguments> graphImplWithLocalAggregationArguments() {
        return crossArguments(
            toArguments(TestSupport::allTypes),
            GraphLoaderMultipleRelTypesAndPropertiesTest::localAggregationArguments
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplWithLocalAggregationArguments")
    void multiplePropertiesWithAggregation(
        Class<? extends GraphFactory> graphFactory,
        Aggregation aggregation,
        double expectedNodeAP1,
        double expectedNodeBP1,
        double expectedNodeAP2,
        double expectedNodeBP2
    ) {
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        runQuery(localDb,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339}]->(a)" +
            ", (b)-[:REL {p1: 45, p2: 1340}]->(b)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(b)");
        GraphsByRelationshipType graphs = TestGraphLoader.from(localDb)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", 1.0, aggregation),
                PropertyMapping.of("p2", "p2", 2.0, aggregation)
            )
            .buildGraphs(graphFactory);

        Graph p1Graph = graphs.getGraph("", Optional.of("p1"));
        Graph expectedP1Graph = fromGdl(String.format(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)",
            expectedNodeAP1,
            expectedNodeBP1
        ));
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph("", Optional.of("p2"));
        Graph expectedP2Graph = fromGdl(String.format(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)",
            expectedNodeAP2,
            expectedNodeBP2
        ));
        assertGraphEquals(expectedP2Graph, p2Graph);
    }

    @AllGraphTypesTest
    void multiplePropertiesWithAggregation_SINGLE(Class<? extends GraphFactory> graphFactory) {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db,
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(b)");
        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", 1.0, SINGLE),
                PropertyMapping.of("p2", "p2", 2.0, SINGLE)
            )
            .buildGraphs(graphFactory);

        String expectedGraphTemplate =
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)";

        Graph p1Graph = graphs.getGraph("", Optional.of("p1"));
        Graph expectedP1GraphOption1 = fromGdl(String.format(expectedGraphTemplate, 43D, 46D));
        Graph expectedP1GraphOption2 = fromGdl(String.format(expectedGraphTemplate, 42D, 46D));
        assertGraphEquals(Arrays.asList(expectedP1GraphOption1, expectedP1GraphOption2), p1Graph);

        Graph p2Graph = graphs.getGraph("", Optional.of("p2"));
        Graph expectedP2GraphOption1 = fromGdl(String.format(expectedGraphTemplate, 1338D, 1341D));
        Graph expectedP2GraphOption2 = fromGdl(String.format(expectedGraphTemplate, 1337D, 1341D));
        assertGraphEquals(Arrays.asList(expectedP2GraphOption1, expectedP2GraphOption2), p2Graph);
    }

    @AllGraphTypesTest
    void graphsByRelationshipTypeCanBeReleased(Class<? extends GraphFactory> graphFactory) {
        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipType("REL1 | REL2")
            .buildGraphs(graphFactory);

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph unionGraph = graphs.getUnion();

        rel1Graph.release();

        assertThrows(NullPointerException.class, () -> rel1Graph.forEachNode(n -> {
            rel1Graph.forEachRelationship(n, (s, t) -> true);
            return true;
        }), "Graph should release");

        unionGraph.release();

        assertThrows(NullPointerException.class, () -> unionGraph.forEachNode(n -> {
            unionGraph.forEachRelationship(n, (s, t) -> true);
            return true;
        }), "UnionGraph should release");
    }

    @AllGraphTypesTest
    void graphsByRelationshipTypeGiveCorrectElementCounts(Class<? extends GraphFactory> graphFactory) {
        GraphsByRelationshipType graphs = TestGraphLoader.from(db)
            .withRelationshipType("REL1 | REL2 | REL3")
            .buildGraphs(graphFactory);

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph unionGraph = graphs.getUnion();

        long[] expectedCounts = new long[4];
        runInTransaction(db, () -> {
            expectedCounts[0] = db.getAllNodes().stream().count();
            expectedCounts[1] = db.getAllRelationships().stream().count();
            // The graphs share the node mapping, so we expect the node count for a subgraph
            // to be equal to the node Count for the entire Neo4j graph
            expectedCounts[2] = db.getAllNodes().stream().count();
            expectedCounts[3] = db
                .getAllRelationships()
                .stream()
                .filter(r -> r.isType(RelationshipType.withName("REL1")))
                .count();
        });
        long unionGraphExpectedNodeCount = expectedCounts[0];
        long unionGraphExpectedRelCount = expectedCounts[1];
        long rel1GraphExpectedNodeCount = expectedCounts[2];
        long rel1GraphExpectedRelCount = expectedCounts[3];

        assertEquals(unionGraphExpectedNodeCount, unionGraph.nodeCount());
        assertEquals(unionGraphExpectedRelCount, unionGraph.relationshipCount());
        assertEquals(rel1GraphExpectedNodeCount, rel1Graph.nodeCount());
        assertEquals(rel1GraphExpectedRelCount, rel1Graph.relationshipCount());
    }
}
