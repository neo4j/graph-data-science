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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

/**
 * Builds a simple test graph.
 *
 * n0 -> n1; n0 -> n2; n1 -> n2
 *
 * @author mknblch
 */
public class SimpleGraphSetup {

    public static final String LABEL = "Node";
    public static final String RELATION = "RELATION";
    public static final String PROPERTY = "weight";

    private final GraphDatabaseService db;

    private long n0, n1, n2;
    private long r0, r1, r2;
    private int v0, v1, v2;

    public SimpleGraphSetup(GraphDatabaseService db) {
        this.db = db;
        setupGraph();
    }

    public SimpleGraphSetup() {
        this.db = TestDatabaseCreator.createTestDatabase();
        setupGraph();
    }

    private void setupGraph() {

        try (Transaction transaction = db.beginTx()) {

            final Node node0 = db.createNode(Label.label(LABEL));
            final Node node1 = db.createNode(Label.label(LABEL));
            final Node node2 = db.createNode(Label.label(LABEL));

            n0 = node0.getId();
            n1 = node1.getId();
            n2 = node2.getId();

            final Relationship rel0 = node0.createRelationshipTo(node1, RelationshipType.withName(RELATION));
            final Relationship rel1 = node0.createRelationshipTo(node2, RelationshipType.withName(RELATION));
            final Relationship rel2 = node1.createRelationshipTo(node2, RelationshipType.withName(RELATION));

            rel0.setProperty(PROPERTY, 1.0);
            rel1.setProperty(PROPERTY, 2.0);
            rel2.setProperty(PROPERTY, 3.0);

            r0 = rel0.getId();
            r1 = rel1.getId();
            r2 = rel2.getId();

            transaction.success();
        }
    }

    public Graph build(Class<? extends GraphFactory> factory) {
        final Graph graph = new GraphLoader((GraphDatabaseAPI) db)
                .withLabel(LABEL)
                .withRelationshipType(RELATION)
                .withRelationshipWeightsFromProperty(PROPERTY, 0.0)
                .load(factory);
        v0 = graph.toMappedNodeId(n0);
        v1 = graph.toMappedNodeId(n1);
        v2 = graph.toMappedNodeId(n2);
        return graph;
    }

    public GraphDatabaseService getDb() {
        return db;
    }

    public int getV0() {
        return v0;
    }

    public int getV1() {
        return v1;
    }

    public int getV2() {
        return v2;
    }

    public long getR0() {
        return r0;
    }

    public long getR1() {
        return r1;
    }

    public long getR2() {
        return r2;
    }

    public long getN0() {
        return n0;
    }

    public long getN1() {
        return n1;
    }

    public long getN2() {
        return n2;
    }

    public void shutdown() {
        if (db != null) db.shutdown();
    }
}
