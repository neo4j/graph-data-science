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

import org.junit.Assume;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;

/**
 * A->B; A->C; B->C;
 *
 *
 *     OutD:   InD: BothD:
 * A:     2      0      2
 * B:     1      1      2
 * C:     0      2      2
 */
class DegreesTest {

    private static final String UNI_DIRECTIONAL = "CREATE" +
                                                  "  (a:Node {name:'a'})" +
                                                  ", (b:Node {name:'b'})" +
                                                  ", (c:Node {name:'c'})" + // shuffled
                                                  ", (a)-[:TYPE]->(b)" +
                                                  ", (a)-[:TYPE]->(c)" +
                                                  ", (b)-[:TYPE]->(c)";

    private static final String BI_DIRECTIONAL = "CREATE" +
                                                 "  (a:Node {name:'a'})" +
                                                 ", (b:Node {name:'b'})" +
                                                 ", (c:Node {name:'c'})" + // shuffled
                                                 ", (a)-[:TYPE]->(b)" +
                                                 ", (b)-[:TYPE]->(a)" +
                                                 ", (a)-[:TYPE]->(c)" +
                                                 ", (c)-[:TYPE]->(a)" +
                                                 ", (b)-[:TYPE]->(c)" +
                                                 ", (c)-[:TYPE]->(b)";

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

    private Graph graph;

    @AllGraphTypesWithoutCypherTest
    void testUnidirectionalOutgoing(Class<? extends GraphFactory> graphFactory) {
        setup(UNI_DIRECTIONAL, Direction.OUTGOING, graphFactory);
        assertEquals(2, graph.degree(nodeId("a"), Direction.OUTGOING));
        assertEquals(1, graph.degree(nodeId("b"), Direction.OUTGOING));
        assertEquals(0, graph.degree(nodeId("c"), Direction.OUTGOING));
    }

    @AllGraphTypesWithoutCypherTest
    void testUnidirectionalIncoming(Class<? extends GraphFactory> graphFactory) {
        setup(UNI_DIRECTIONAL, Direction.INCOMING, graphFactory);
        assertEquals(0, graph.degree(nodeId("a"), Direction.INCOMING));
        assertEquals(1, graph.degree(nodeId("b"), Direction.INCOMING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.INCOMING));
    }

    @AllGraphTypesWithoutCypherTest
    void testUnidirectionalUndirected(Class<? extends GraphFactory> graphFactory) {

        setup(UNI_DIRECTIONAL, Direction.BOTH, graphFactory);
        assertEquals(2, graph.degree(nodeId("a"), Direction.BOTH));
        assertEquals(2, graph.degree(nodeId("b"), Direction.BOTH));
        assertEquals(2, graph.degree(nodeId("c"), Direction.BOTH));
    }

    @AllGraphTypesWithoutCypherTest
    void testBidirectionalOutgoing(Class<? extends GraphFactory> graphFactory) {
        setup(BI_DIRECTIONAL, Direction.OUTGOING, graphFactory);
        assertEquals(2, graph.degree(nodeId("a"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("b"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.OUTGOING));
    }

    @AllGraphTypesWithoutCypherTest
    void testBidirectionalIncoming(Class<? extends GraphFactory> graphFactory) {
        setup(BI_DIRECTIONAL, Direction.INCOMING, graphFactory);
        assertEquals(2, graph.degree(nodeId("a"), Direction.INCOMING));
        assertEquals(2, graph.degree(nodeId("b"), Direction.INCOMING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.INCOMING));
    }

    @AllGraphTypesWithoutCypherTest
    void testBidirectionalBoth(Class<? extends GraphFactory> graphFactory) {
        Assume.assumeFalse(graphFactory.isAssignableFrom(GraphViewFactory.class));
        setup(BI_DIRECTIONAL, Direction.BOTH, graphFactory);
        assertEquals(4, graph.degree(nodeId("a"), Direction.BOTH));
        assertEquals(4, graph.degree(nodeId("b"), Direction.BOTH));
        assertEquals(4, graph.degree(nodeId("c"), Direction.BOTH));
    }

    @AllGraphTypesWithoutCypherTest
    void testBidirectionalUndirected(Class<? extends GraphFactory> graphFactory) {
        setup(BI_DIRECTIONAL, null, graphFactory);
        assertEquals(2, graph.degree(nodeId("a"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("b"), Direction.OUTGOING));
        assertEquals(2, graph.degree(nodeId("c"), Direction.OUTGOING));
    }

    private void setup(
            String cypher,
            Direction direction,
            Class<? extends GraphFactory> graphFactory) {
        DB.execute(cypher);
        GraphLoader graphLoader = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .withDirection(direction == null ? Direction.BOTH : direction);
        if (direction == null) {
            graphLoader.undirected();
        }
        graph = graphLoader.load(graphFactory);
    }

    private long nodeId(String name) {
        try (Transaction ignored = DB.beginTx()) {
            return graph.toMappedNodeId(DB.findNodes(Label.label("Node"), "name", name).next().getId());
        }
    }
}
