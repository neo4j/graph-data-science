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
import org.junit.jupiter.api.function.Executable;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.neo4jview.GraphView;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class RelationshipWeightImportTest {

    private Graph graph;

    private GraphDatabaseAPI db;

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testWeightsOfInterconnectedNodesWithOutgoing(Class<? extends GraphFactory> graphFactory) {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.OUTGOING, graphFactory);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
    }

    @AllGraphTypesWithoutCypherTest
    void testWeightsOfTriangledNodesWithOutgoing(Class<? extends GraphFactory> graphFactory) {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.OUTGOING, graphFactory);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
        checkWeight(2, Direction.OUTGOING, 3.0);
    }

    @AllGraphTypesWithoutCypherTest
    void testWeightsOfInterconnectedNodesWithIncoming(Class<? extends GraphFactory> graphFactory) {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.INCOMING, graphFactory);

        checkWeight(0, Direction.INCOMING, 2.0);
        checkWeight(1, Direction.INCOMING, 1.0);
    }

    @AllGraphTypesWithoutCypherTest
    void testWeightsOfTriangledNodesWithIncoming(Class<? extends GraphFactory> graphFactory) {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.INCOMING, graphFactory);

        checkWeight(0, Direction.INCOMING, 3.0);
        checkWeight(1, Direction.INCOMING, 1.0);
        checkWeight(2, Direction.INCOMING, 2.0);
    }

    @AllGraphTypesWithoutCypherTest
    void testWeightsOfInterconnectedNodesWithBoth(Class<? extends GraphFactory> graphFactory) {
        setup("CREATE (a:N),(b:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(a)", Direction.BOTH, graphFactory);

        // loading both overwrites the weights in the following order,
        // which expects the GraphFactory to load OUTGOINGs before INCOMINGs
        //   (a)-[{w:1}]->(b)  |  (a)<-[{w:2}]-(b)  |  (b)-[{w:2}]->(a)  |  (b)<-[{w:1}]-(a)
        // therefore the final weight for in/outs of either a/b is 1,
        // the weight of 2 is discarded.
        // This cannot be represented in the graph view
        assumeFalse(graph instanceof GraphView, "GraphView is not able to represent the test case");

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);

        checkWeight(0, Direction.INCOMING, 2.0);
        checkWeight(1, Direction.INCOMING, 1.0);

        checkWeight(0, Direction.BOTH, new double[]{1.0, 1.0}, 1.0, 2.0);
        checkWeight(1, Direction.BOTH, new double[]{2.0, 2.0}, 2.0, 1.0);
    }

    @AllGraphTypesWithoutCypherTest
    void testWeightsOfTriangledNodesWithBoth(Class<? extends GraphFactory> graphFactory) {
        setup("CREATE (a:N),(b:N),(c:N) CREATE (a)-[:R{w:1}]->(b),(b)-[:R{w:2}]->(c),(c)-[:R{w:3}]->(a)", Direction.BOTH, graphFactory);

        checkWeight(0, Direction.OUTGOING, 1.0);
        checkWeight(1, Direction.OUTGOING, 2.0);
        checkWeight(2, Direction.OUTGOING, 3.0);

        checkWeight(0, Direction.INCOMING, 3.0);
        checkWeight(1, Direction.INCOMING, 1.0);
        checkWeight(2, Direction.INCOMING, 2.0);

        checkWeight(0, Direction.BOTH, new double[]{1.0, 0.0}, 1.0, 3.0);
        checkWeight(1, Direction.BOTH, new double[]{2.0, 0.0}, 2.0, 1.0);
        checkWeight(2, Direction.BOTH, new double[]{3.0, 0.0}, 3.0, 2.0);
    }

    private void setup(
            String cypher,
            Direction direction,
            Class<? extends GraphFactory> graphFactory) {
        db.execute(cypher);
        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withDirection(direction)
                .withRelationshipProperties(PropertyMapping.of("w", 0.0))
                .sorted()
                .load(graphFactory);
    }

    private void checkWeight(int nodeId, Direction direction, double... expecteds) {
        List<Executable> assertions = new ArrayList<>();
        graph.forEachRelationship(nodeId, direction, checks(direction, expecteds, expecteds, assertions));
        assertAll(assertions);
    }

    private void checkWeight(int nodeId, Direction direction, double[] expectedFromGraph, double... expectedFromIterator) {
        List<Executable> assertions = new ArrayList<>();
        graph.forEachRelationship(nodeId, direction, checks(direction, expectedFromIterator, expectedFromGraph, assertions));
        assertAll(assertions);
    }

    private WeightedRelationshipConsumer checks(Direction direction, double[] expectedFromIterator, double[] expectedFromGraph, List<Executable> assertions) {
        AtomicInteger i = new AtomicInteger();
        int limit = Math.min(expectedFromIterator.length, expectedFromGraph.length);
        return (s, t, w) -> {
            String rel = String.format("(%d %s %d)", s, arrow(direction), t);
            if (i.get() >= limit) {
                assertions.add(() -> assertFalse(i.get() >= limit, String.format("Unexpected relationship: %s = %.1f", rel, w)));
                return false;
            }
            double actual = (direction == Direction.INCOMING) ? graph.weightOf(t, s) : graph.weightOf(s, t);
            final int index = i.getAndIncrement();
            double expectedIterator = expectedFromIterator[index];
            double expectedGraph = expectedFromGraph[index];
            assertions.add(() -> assertEquals(expectedGraph, actual, 1e-4, String.format("%s (RI+W): %.1f != %.1f", rel, actual, expectedGraph)));
            assertions.add(() -> assertEquals(expectedIterator, w, 1e-4, String.format("%s (WRI): %.1f != %.1f", rel, w, expectedIterator)));
            return true;
        };
    }

    private static String arrow(Direction direction) {
        switch (direction) {
            case OUTGOING:
                return "->";
            case INCOMING:
                return "<-";
            case BOTH:
                return "<->";
            default:
                return "???";
        }
    }
}
