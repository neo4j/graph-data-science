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
package org.neo4j.graphalgo.linkprediction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

public class NeighborsFinderTest extends AlgoTestBase {

    public static final RelationshipType FRIEND = RelationshipType.withName("FRIEND");
    public static final RelationshipType COLLEAGUE = RelationshipType.withName("COLLEAGUE");
    public static final RelationshipType FOLLOWS = RelationshipType.withName("FOLLOWS");

    @BeforeEach
    public void setup() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @Test
    public void excludeDirectRelationships() {
        runInTransaction(db, () -> {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            node1.createRelationshipTo(node2, FRIEND);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Node node1 = db.getNodeById(0);
            Node node2 = db.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(0, neighbors.size());
        });
    }

    @Test
    public void sameNodeHasNoCommonNeighbors() {
        runInTransaction(db, () -> {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            node1.createRelationshipTo(node2, FRIEND);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Node node1 = db.getNodeById(0);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node1, null, Direction.BOTH);

            assertEquals(0, neighbors.size());
        });
    }

    @Test
    public void findNeighborsExcludingDirection() {
        runInTransaction(db, () -> {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();
            Node node4 = db.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Node node1 = db.getNodeById(0);
            Node node2 = db.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, null, Direction.BOTH);

            assertEquals(2, neighbors.size());
        });
    }

    @Test
    public void findOutgoingNeighbors() {
        runInTransaction(db, () -> {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();

            node1.createRelationshipTo(node3, FOLLOWS);
            node2.createRelationshipTo(node3, FOLLOWS);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Node node1 = db.getNodeById(0);
            Node node2 = db.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, FOLLOWS, Direction.OUTGOING);

            assertEquals(1, neighbors.size());
        });
    }

    @Test
    public void findIncomingNeighbors() {
        runInTransaction(db, () -> {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();

            node3.createRelationshipTo(node1, FOLLOWS);
            node3.createRelationshipTo(node2, FOLLOWS);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Node node1 = db.getNodeById(0);
            Node node2 = db.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, FOLLOWS, Direction.INCOMING);

            assertEquals(1, neighbors.size());
        });
    }

    @Test
    public void findNeighborsOfSpecificRelationshipType() {
        runInTransaction(db, () -> {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Node node3 = db.createNode();
            Node node4 = db.createNode();

            node1.createRelationshipTo(node3, FRIEND);
            node2.createRelationshipTo(node3, FRIEND);
            node1.createRelationshipTo(node4, COLLEAGUE);
            node2.createRelationshipTo(node4, COLLEAGUE);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Node node1 = db.getNodeById(0);
            Node node2 = db.getNodeById(1);
            Set<Node> neighbors = neighborsFinder.findCommonNeighbors(node1, node2, COLLEAGUE, Direction.BOTH);

            assertEquals(1, neighbors.size());
        });
    }

    @Test
    public void dontCountDuplicates() {
        Node[] nodes = new Node[4];
        runInTransaction(db, () -> {
            nodes[0] = db.createNode();
            nodes[1] = db.createNode();
            nodes[2] = db.createNode();
            nodes[3] = db.createNode();

            nodes[0].createRelationshipTo(nodes[2], FRIEND);
            nodes[1].createRelationshipTo(nodes[2], FRIEND);
            nodes[0].createRelationshipTo(nodes[3], COLLEAGUE);
            nodes[1].createRelationshipTo(nodes[3], COLLEAGUE);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Set<Node> neighbors = neighborsFinder.findNeighbors(nodes[0], nodes[1], null, Direction.BOTH);

            assertEquals(2, neighbors.size());
            assertThat(neighbors, hasItems(nodes[2], nodes[3]));
        });
    }

    @Test
    public void otherNodeCountsAsNeighbor() {
        Node[] nodes = new Node[2];
        runInTransaction(db, () -> {
            nodes[0] = db.createNode();
            nodes[1] = db.createNode();
            nodes[0].createRelationshipTo(nodes[1], FRIEND);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Set<Node> neighbors = neighborsFinder.findNeighbors(nodes[0], nodes[1], null, Direction.BOTH);

            assertEquals(2, neighbors.size());
            assertThat(neighbors, hasItems(nodes[0], nodes[1]));
        });
    }

    @Test
    public void otherNodeCountsAsOutgoingNeighbor() {
        Node[] nodes = new Node[2];
        runInTransaction(db, () -> {
            nodes[0] = db.createNode();
            nodes[1] = db.createNode();
            nodes[0].createRelationshipTo(nodes[1], FRIEND);
        });

        NeighborsFinder neighborsFinder = new NeighborsFinder(db);

        runInTransaction(db, () -> {
            Set<Node> neighbors = neighborsFinder.findNeighbors(nodes[0], nodes[1], null, Direction.OUTGOING);

            assertEquals(1, neighbors.size());
            assertThat(neighbors, hasItems(nodes[1]));
        });
    }
}

