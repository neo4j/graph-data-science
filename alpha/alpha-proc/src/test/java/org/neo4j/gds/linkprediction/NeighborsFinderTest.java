/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.linkprediction;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoTestBase;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;

class NeighborsFinderTest extends AlgoTestBase {

    private static final RelationshipType FRIEND = RelationshipType.withName("FRIEND");
    private static final RelationshipType COLLEAGUE = RelationshipType.withName("COLLEAGUE");
    private static final RelationshipType FOLLOWS = RelationshipType.withName("FOLLOWS");

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.track_cursor_close", "false"));
        builder.setConfigRaw(Map.of("unsupported.dbms.debug.trace_cursors", "false"));
    }

    @Test
    void excludeDirectRelationships() {
        runInTransaction(db, tx -> {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, FRIEND);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder();

        runInTransaction(db, tx -> {
            Node node1 = tx.getNodeById(0);
            Node node2 = tx.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(0, neighbors.size());
        });
    }

    @Test
    void sameNodeHasNoCommonNeighbors() {
        runInTransaction(db, tx -> {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            node1.createRelationshipTo(node2, FRIEND);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder();

        runInTransaction(db, tx -> {
            Node node1 = tx.getNodeById(0);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node1, null, Direction.BOTH);

            assertEquals(0, neighbors.size());
        });
    }

    @Test
    void findNeighborsExcludingDirection() {
        runInTransaction(db, tx -> {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Node node3 = tx.createNode();
            Node node4 = tx.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder();

        runInTransaction(db, tx -> {
            Node node1 = tx.getNodeById(0);
            Node node2 = tx.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(2, neighbors.size());
        });
    }

    @Test
    void findOutgoingNeighbors() {
        runInTransaction(db, tx -> {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Node node3 = tx.createNode();

            node1.createRelationshipTo(node3, FOLLOWS);
            node2.createRelationshipTo(node3, FOLLOWS);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder();

        runInTransaction(db, tx -> {
            Node node1 = tx.getNodeById(0);
            Node node2 = tx.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, FOLLOWS, Direction.OUTGOING);

            assertEquals(1, neighbors.size());
        });
    }

    @Test
    void findIncomingNeighbors() {
        runInTransaction(db, tx -> {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Node node3 = tx.createNode();

            node3.createRelationshipTo(node1, FOLLOWS);
            node3.createRelationshipTo(node2, FOLLOWS);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder();

        runInTransaction(db, tx -> {
            Node node1 = tx.getNodeById(0);
            Node node2 = tx.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, FOLLOWS, Direction.INCOMING);

            assertEquals(1, neighbors.size());
        });
    }

    @Test
    void findNeighborsOfSpecificRelationshipType() {
        runInTransaction(db, tx -> {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Node node3 = tx.createNode();
            Node node4 = tx.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder();

        runInTransaction(db, tx -> {
            Node node1 = tx.getNodeById(0);
            Node node2 = tx.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, COLLEAGUE, Direction.BOTH);

            assertEquals(1, neighbors.size());
        });
    }

    @Test
    void dontCountDuplicates() {
        runInTransaction(db, tx -> {
            Node[] nodes = new Node[4];
            nodes[0] = tx.createNode();
            nodes[1] = tx.createNode();
            nodes[2] = tx.createNode();
            nodes[3] = tx.createNode();

            nodes[0].createRelationshipTo(nodes[2], FRIEND);
            nodes[1].createRelationshipTo(nodes[2], FRIEND);
            nodes[0].createRelationshipTo(nodes[3], COLLEAGUE);
            nodes[1].createRelationshipTo(nodes[3], COLLEAGUE);

            NeighborsFinder neighborsFinder = new NeighborsFinder();
            Set<Node> neighbors = neighborsFinder.findNeighbors(nodes[0], nodes[1], null, Direction.BOTH);

            assertEquals(2, neighbors.size());
            assertThat(neighbors, hasItems(nodes[2], nodes[3]));
        });
    }

    @Test
    void otherNodeCountsAsNeighbor() {
        runInTransaction(db, tx -> {
            Node[] nodes = new Node[2];
            nodes[0] = tx.createNode();
            nodes[1] = tx.createNode();
            nodes[0].createRelationshipTo(nodes[1], FRIEND);

            NeighborsFinder neighborsFinder = new NeighborsFinder();
            Set<Node> neighbors = neighborsFinder.findNeighbors(nodes[0], nodes[1], null, Direction.BOTH);

            assertEquals(2, neighbors.size());
            assertThat(neighbors, hasItems(nodes[0], nodes[1]));
        });


    }

    @Test
    void otherNodeCountsAsOutgoingNeighbor() {
        Node[] nodes = new Node[2];
        runInTransaction(db, tx -> {
            nodes[0] = tx.createNode();
            nodes[1] = tx.createNode();
            nodes[0].createRelationshipTo(nodes[1], FRIEND);

            NeighborsFinder neighborsFinder = new NeighborsFinder();
            Set<Node> neighbors = neighborsFinder.findNeighbors(nodes[0], nodes[1], null, Direction.OUTGOING);

            assertEquals(1, neighbors.size());
            assertThat(neighbors, hasItems(nodes[1]));
        });
    }
}

