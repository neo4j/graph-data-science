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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public abstract class Neo4jTestCase {

    public static final String LABEL = "Node";
    public static final String WEIGHT_PROPERTY = "weight";
    public static final String RELATION = "RELATION";

    protected static GraphDatabaseService db;

    @BeforeAll
    public static void setup() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterAll
    public static void teardown() {
        if (db != null) {
            db.shutdown();
            db = null;
        }
    }

    public static int newNode() {
        try (Transaction transaction = db.beginTx()) {
            final Node node = db.createNode(Label.label(LABEL));
            transaction.success();
            final int id = Math.toIntExact(node.getId());
            transaction.success();
            return id;
        }
    }

    public static long newRelation(int sourceNodeId, int targetNodeId) {
        try (Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceNodeId);
            final Node target = db.getNodeById(targetNodeId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName(RELATION));
            transaction.success();
            return relation.getId();
        }
    }

    public static long newRelation(
            long sourceNodeId,
            long targetNodeId,
            double weight) {
        try (Transaction transaction = db.beginTx()) {
            final Node source = db.getNodeById(sourceNodeId);
            final Node target = db.getNodeById(targetNodeId);
            final Relationship relation = source.createRelationshipTo(
                    target,
                    RelationshipType.withName(RELATION));
            relation.setProperty(WEIGHT_PROPERTY, weight);
            transaction.success();
            return relation.getId();
        }
    }
}
