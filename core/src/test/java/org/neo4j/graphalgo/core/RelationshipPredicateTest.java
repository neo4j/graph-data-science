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
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

/**
 *
 *      (A)-->(B)-->(C)
 *       ^           |
 *       °-----------°
 */
class RelationshipPredicateTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(a)";

    private static final Label LABEL = Label.label("Node");

    private static long nodeA;
    private static long nodeB;
    private static long nodeC;

    private GraphDatabaseAPI db;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, DB_CYPHER);
        try (Transaction tx = db.beginTx()) {
            nodeA = db.findNode(LABEL, "name", "a").getId();
            nodeB = db.findNode(LABEL, "name", "b").getId();
            nodeC = db.findNode(LABEL, "name", "c").getId();
            tx.success();
        }
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testOutgoing(Class<? extends GraphFactory> graphFactory) {

        final Graph graph = loader()
                .withDirection(Direction.OUTGOING)
                .sorted()
                .load(graphFactory);

        // A -> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB)
        ));

        // B -> A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA)
        ));

        // B -> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC)
        ));

        // C -> B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB)
        ));

        // C -> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA)
        ));

        // A -> C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC)
        ));
    }


    @AllGraphTypesWithoutCypherTest
    void testIncoming(Class<? extends GraphFactory> graphFactory) {

        final Graph graph = loader()
                .withDirection(Direction.INCOMING)
                .sorted()
                .load(graphFactory);

        // B <- A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));

        // A <- B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // C <- B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // B <- C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));


        // A <- C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));

        // C <- A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));
    }


    @AllGraphTypesWithoutCypherTest
    void testBoth(Class<? extends GraphFactory> graphFactory) {

        final Graph graph = loader()
                .withDirection(Direction.BOTH)
                .sorted()
                .load(graphFactory);

        // A -> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB)
        ));

        // B -> A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA)
        ));


        // B -> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC)
        ));

        // C -> B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB)
        ));

        // C -> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA)
        ));

        // A -> C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC)
        ));


        // B <- A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));

        // A <- B
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // C <- B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB),
                Direction.INCOMING
        ));

        // B <- C
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));

        // A <- C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC),
                Direction.INCOMING
        ));

        // C <- A
        assertFalse(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA),
                Direction.INCOMING
        ));

        // A <-> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeB),
                Direction.BOTH
        ));

        // B <-> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeA),
                Direction.BOTH
        ));

        // B <-> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeB),
                graph.toMappedNodeId(nodeC),
                Direction.BOTH
        ));

        // C <-> B
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeB),
                Direction.BOTH
        ));

        // C <-> A
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeC),
                graph.toMappedNodeId(nodeA),
                Direction.BOTH
        ));

        // A <-> C
        assertTrue(graph.exists(
                graph.toMappedNodeId(nodeA),
                graph.toMappedNodeId(nodeC),
                Direction.BOTH
        ));
    }

    private GraphLoader loader() {
        return new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType();
    }
}
