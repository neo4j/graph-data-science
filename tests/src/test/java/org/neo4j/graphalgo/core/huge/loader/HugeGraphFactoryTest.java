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
package org.neo4j.graphalgo.core.huge.loader;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.DeduplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.GraphHelper.collectTargetIds;
import static org.neo4j.graphalgo.GraphHelper.collectTargetWeights;


class HugeGraphFactoryTest {

    private static GraphDatabaseAPI DB;

    private static long id1;
    private static long id2;
    private static long id3;

    @BeforeAll
    static void setup() {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute("CREATE " +
                   "(n1:Node1 {prop1: 1})," +
                   "(n2:Node2 {prop2: 2})," +
                   "(n3:Node3 {prop3: 3})" +
                   "CREATE " +
                   "(n1)-[:REL1 {prop1: 1}]->(n2)," +
                   "(n1)-[:REL2 {prop2: 2}]->(n3)," +
                   "(n2)-[:REL1 {prop3: 3, weight: 42}]->(n3)," +
                   "(n2)-[:REL3 {prop4: 4, weight: 1337}]->(n3);");
        id1 = DB.execute("MATCH (n:Node1) RETURN id(n) AS id").<Long>columnAs("id").next();
        id2 = DB.execute("MATCH (n:Node2) RETURN id(n) AS id").<Long>columnAs("id").next();
        id3 = DB.execute("MATCH (n:Node3) RETURN id(n) AS id").<Long>columnAs("id").next();
    }

    @AfterAll
    static void tearDown() {
        if (DB != null) DB.shutdown();
    }

    @Test
    void testAnyLabel() {
        final Graph graph = new GraphLoader(DB)
                .withAnyLabel()
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class);

        assertEquals(3L, graph.nodeCount());
    }

    @Test
    void testWithLabel() {
        final Graph graph = new GraphLoader(DB)
                .withLabel("Node1")
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class);

        assertEquals(1L, graph.nodeCount());
    }

    @Test
    void testAnyRelation() {
        final Graph graph = new GraphLoader(DB)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .load(HugeGraphFactory.class);

        long[] out1 = collectTargetIds(graph, id1);
        assertArrayEquals(expectedIds(graph, id2, id3), out1);

        long[] out2 = collectTargetIds(graph, id2);
        assertArrayEquals(expectedIds(graph, id3), out2);
    }

    @Test
    void testWithBothWeightedRelationship() {
        final Graph graph = new GraphLoader(DB)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withRelationshipType("REL3")
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .withDirection(Direction.BOTH)
                .load(HugeGraphFactory.class);

        assertEquals(2, graph.relationshipCount());

        long[] targets = collectTargetIds(graph, id2);
        assertArrayEquals(expectedIds(graph, id3), targets);
        double[] weights = collectTargetWeights(graph, id2);
        assertArrayEquals(expectedWeights(1337), weights, 1e-4);
    }

    @Test
    void testWithOutgoingRelationship() {
        final Graph graph = new GraphLoader(DB)
                .withAnyLabel()
                .withoutRelationshipWeights()
                .withRelationshipType("REL3")
                .withDirection(Direction.OUTGOING)
                .load(HugeGraphFactory.class);

        assertEquals(1, graph.relationshipCount());

        long[] out1 = collectTargetIds(graph, id2);
        assertArrayEquals(expectedIds(graph, id3), out1);
    }

    @Test
    void testWithProperty() {

        final Graph graph = new GraphLoader(DB)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("prop1", 1337.42)
                .load(HugeGraphFactory.class);

        double[] out1 = collectTargetWeights(graph, id1);
        assertArrayEquals(expectedWeights(1.0, 1337.42), out1, 1e-4);
    }

    @Test
    void testWithNodeProperties() {
        final Graph graph = new GraphLoader(DB)
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .withOptionalNodeProperties(
                        PropertyMapping.of("prop1", "prop1", 0D),
                        PropertyMapping.of("prop2", "prop2", 0D),
                        PropertyMapping.of("prop3", "prop3", 0D)
                )
                .load(HugeGraphFactory.class);

        assertEquals(1.0, graph.nodeProperties("prop1").nodeWeight((int) graph.toMappedNodeId(0L)), 0.01);
        assertEquals(2.0, graph.nodeProperties("prop2").nodeWeight((int) graph.toMappedNodeId(1L)), 0.01);
        assertEquals(3.0, graph.nodeProperties("prop3").nodeWeight((int) graph.toMappedNodeId(2L)), 0.01);
    }

    @Test
    void testWithHugeNodeProperties() {
        final Graph graph = new GraphLoader(DB)
                .withoutRelationshipWeights()
                .withAnyRelationshipType()
                .withOptionalNodeProperties(
                        PropertyMapping.of("prop1", "prop1", 0D),
                        PropertyMapping.of("prop2", "prop2", 0D),
                        PropertyMapping.of("prop3", "prop3", 0D)
                )
                .load(HugeGraphFactory.class);

        assertEquals(1.0, graph.nodeProperties("prop1").nodeWeight(graph.toMappedNodeId(0L)), 0.01);
        assertEquals(2.0, graph.nodeProperties("prop2").nodeWeight(graph.toMappedNodeId(1L)), 0.01);
        assertEquals(3.0, graph.nodeProperties("prop3").nodeWeight(graph.toMappedNodeId(2L)), 0.01);
    }

    @Test
    void stopsImportingWhenTransactionHasBeenTerminated() {
        TerminationFlag terminationFlag = () -> false;
        TransactionTerminatedException exception = assertThrows(
                TransactionTerminatedException.class,
                () -> {
                    new GraphLoader(DB)
                            .withTerminationFlag(terminationFlag)
                            .load(HugeGraphFactory.class);
                });
        assertEquals(Status.Transaction.Terminated, exception.status());
    }

    @Test
    void testLoadDuplicateRelationships() {
        final Graph graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withoutRelationshipWeights()
                .withDeduplicateRelationshipsStrategy(DeduplicateRelationshipsStrategy.NONE)
                .load(HugeGraphFactory.class);

        long[] out2 = collectTargetIds(graph, id2);
        assertArrayEquals(expectedIds(graph, id3, id3), out2);
    }

    @Test
    void testLoadDuplicateRelationshipsWithWeights() {
        final Graph graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .withDeduplicateRelationshipsStrategy(DeduplicateRelationshipsStrategy.NONE)
                .load(HugeGraphFactory.class);

        double[] out1 = collectTargetWeights(graph, id2);
        assertArrayEquals(expectedWeights(42.0, 1337.0), out1, 1e-4);
    }

    @ParameterizedTest
    @CsvSource({"SKIP, 42.0", "SUM, 1379.0", "MAX, 1337.0", "MIN, 42.0"})
    void testLoadDuplicateRelationshipsWithWeightsAggregation(
            DeduplicateRelationshipsStrategy deduplicateRelationshipsStrategy,
            double expectedWeight) {
        final Graph graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .withDeduplicateRelationshipsStrategy(deduplicateRelationshipsStrategy)
                .load(HugeGraphFactory.class);

        double[] out1 = collectTargetWeights(graph, id2);
        assertArrayEquals(expectedWeights(expectedWeight), out1, 1e-4);
    }

    private long[] expectedIds(final Graph graph, long... expected) {
        return Arrays.stream(expected).map(graph::toMappedNodeId).sorted().toArray();
    }

    private double[] expectedWeights(double... expected) {
        return expected;
    }
}
