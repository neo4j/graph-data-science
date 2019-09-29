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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithMultipleRelTypeSupportTest;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.MultipleRelTypesSupport;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphByType;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.GraphHelper.assertOutProperties;
import static org.neo4j.graphalgo.GraphHelper.assertOutPropertiesWithDelta;
import static org.neo4j.graphalgo.GraphHelper.assertOutRelationships;
import static org.neo4j.graphalgo.GraphHelper.collectTargetIds;
import static org.neo4j.graphalgo.GraphHelper.collectTargetProperties;
import static org.neo4j.graphalgo.TestSupport.toArguments;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.MAX;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.MIN;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.SKIP;
import static org.neo4j.graphalgo.core.DeduplicationStrategy.SUM;
import static org.neo4j.helpers.collection.Iterables.asSet;

class GraphLoaderPropertiesTest {

    public static final String DB_CYPHER =
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
        db.execute(DB_CYPHER);
        id1 = db.execute("MATCH (n:Node1) RETURN id(n) AS id").<Long>columnAs("id").next();
        id2 = db.execute("MATCH (n:Node2) RETURN id(n) AS id").<Long>columnAs("id").next();
        id3 = db.execute("MATCH (n:Node3) RETURN id(n) AS id").<Long>columnAs("id").next();
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testAnyLabel(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(graphFactory);

        assertEquals(3L, graph.nodeCount());
    }

    @AllGraphTypesWithoutCypherTest
    void testWithLabel(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withLabel("Node1")
                .withAnyRelationshipType()
                .load(graphFactory);

        assertEquals(1L, graph.nodeCount());
    }

    @AllGraphTypesWithoutCypherTest
    void testAnyRelation(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(graphFactory);

        assertOutRelationships(graph, id1, id2, id3);
        assertOutRelationships(graph, id2, id3);
    }

    @AllGraphTypesWithoutCypherTest
    void testWithBothWeightedRelationship(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyLabel()
                .withRelationshipType("REL3")
                .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                .withDirection(Direction.BOTH)
                .load(graphFactory);

        assertEquals(2, graph.relationshipCount());
        assertOutRelationships(graph, id2, id3);
        assertOutPropertiesWithDelta(graph, 1e-4, id2, 1337);
    }

    @AllGraphTypesWithoutCypherTest
    void testWithOutgoingRelationship(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyLabel()
                .withRelationshipType("REL3")
                .withDirection(Direction.OUTGOING)
                .load(graphFactory);

        assertEquals(1, graph.relationshipCount());
        assertOutRelationships(graph, id2, id3);
    }

    @AllGraphTypesWithoutCypherTest
    void testWithProperty(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(PropertyMapping.of("prop1", 1337.42))
                .load(graphFactory);

        assertOutPropertiesWithDelta(graph, 1e-4, id1, 1.0, 1337.42);
    }

    @AllGraphTypesWithoutCypherTest
    void testWithNodeProperties(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withOptionalNodeProperties(
                        PropertyMapping.of("prop1", "prop1", 0D),
                        PropertyMapping.of("prop2", "prop2", 0D),
                        PropertyMapping.of("prop3", "prop3", 0D)
                )
                .load(graphFactory);

        assertEquals(1.0, graph.nodeProperties("prop1").nodeWeight(graph.toMappedNodeId(0L)), 0.01);
        assertEquals(2.0, graph.nodeProperties("prop2").nodeWeight(graph.toMappedNodeId(1L)), 0.01);
        assertEquals(3.0, graph.nodeProperties("prop3").nodeWeight(graph.toMappedNodeId(2L)), 0.01);
    }

    @AllGraphTypesWithoutCypherTest
    void testWithHugeNodeProperties(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withOptionalNodeProperties(
                        PropertyMapping.of("prop1", "prop1", 0D),
                        PropertyMapping.of("prop2", "prop2", 0D),
                        PropertyMapping.of("prop3", "prop3", 0D)
                )
                .load(graphFactory);

        assertEquals(1.0, graph.nodeProperties("prop1").nodeWeight(graph.toMappedNodeId(0L)), 0.01);
        assertEquals(2.0, graph.nodeProperties("prop2").nodeWeight(graph.toMappedNodeId(1L)), 0.01);
        assertEquals(3.0, graph.nodeProperties("prop3").nodeWeight(graph.toMappedNodeId(2L)), 0.01);
    }

    @AllGraphTypesWithoutCypherTest
    void stopsImportingWhenTransactionHasBeenTerminated(Class<? extends GraphFactory> graphFactory) {
        TerminationFlag terminationFlag = () -> false;
        TransactionTerminatedException exception = assertThrows(
                TransactionTerminatedException.class,
                () -> {
                    new GraphLoader(db)
                            .withTerminationFlag(terminationFlag)
                            .load(graphFactory);
                });
        assertEquals(Status.Transaction.Terminated, exception.status());
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    void testLoadDuplicateRelationships(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withDeduplicationStrategy(DeduplicationStrategy.NONE)
                .load(graphFactory);

        assertOutRelationships(graph, id2, id3, id3);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    void testLoadDuplicateRelationshipsWithWeights(Class<? extends GraphFactory> graphFactory) {
        final Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                .withDeduplicationStrategy(DeduplicationStrategy.NONE)
                .load(graphFactory);

        assertOutPropertiesWithDelta(graph, 1e-4, id2, 42.0, 1337.0);
    }

    static Stream<Arguments> deduplicateWithWeightsParams() {
        return TestSupport.crossArguments(toArguments(TestSupport::allTypesWithMultipleRelTypeSupport), () -> Stream.of(
                Arguments.of(SKIP, 42.0),
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
            double expectedWeight) {
        final Graph graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
                .withDeduplicationStrategy(deduplicationStrategy)
                .load(graphFactory);

        assertOutPropertiesWithDelta(graph, 1e-4, id2, expectedWeight);
    }

    @AllGraphTypesWithMultipleRelTypeSupportTest
    <T extends GraphFactory & MultipleRelTypesSupport>
    void testLoadMultipleRelationships(Class<T> graphFactory) {
        GraphByType graphs = new GraphLoader(db)
                .withAnyLabel()
                .withRelationshipType("REL1 | REL2")
                .build(graphFactory)
                .loadGraphsByRelType();

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
        GraphByType graphs = new GraphLoader(db)
                .withAnyLabel()
                .withRelationshipType("REL1 | REL2")
                .withRelationshipProperties(PropertyMapping.of("prop1", 42D))
                .build(HugeGraphFactory.class)
                .loadGraphsByRelType();

        assertEquals(2, graphs.availableRelationshipTypes().size());
        assertEquals(graphs.availableRelationshipTypes(), asSet(asList("REL1", "REL2")));

        Graph rel1Graph = graphs.getGraph("REL1");
        Graph rel2Graph = graphs.getGraph("REL2");
        Graph unionGraph = graphs.getGraph("REL1 | REL2");

        assertOutProperties(rel1Graph, id1, 1D);
        assertOutProperties(rel2Graph, id1, 42D);
        assertOutProperties(unionGraph, id1, 1D, 42D);
    }

}
