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
package org.neo4j.graphalgo.datasets;

import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.neo4j.graphalgo.datasets.CoraSchema.CITES_TYPE;
import static org.neo4j.graphalgo.datasets.CoraSchema.EXT_ID_NODE_PROPERTY;
import static org.neo4j.graphalgo.datasets.CoraSchema.PAPER_LABEL;
import static org.neo4j.graphalgo.datasets.CoraSchema.SUBJECT_NODE_PROPERTY;
import static org.neo4j.graphalgo.datasets.CoraSchema.TEST_TYPE;
import static org.neo4j.graphalgo.datasets.CoraSchema.TRAIN_TYPE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public abstract class AbstractCora extends Dataset {

    private static final RelationshipType TRAIN_RELATIONSHIP_TYPE = RelationshipType.withName(TRAIN_TYPE);
    private static final RelationshipType TEST_RELATIONSHIP_TYPE = RelationshipType.withName(TEST_TYPE);
    private static final RelationshipType CITES_RELATIONSHIP_TYPE = RelationshipType.withName(CITES_TYPE);

    AbstractCora(String id) {
        super(id);
    }

    abstract String coraNodesFile();

    abstract String trainRelationshipsFile();

    abstract String testRelationshipsFile();

    abstract String citesRelationshipsFile();

    public static int numberOfFeatures(String datasetId) {
        switch (datasetId) {
            case Cora.ID: return Cora.numberOfFeatures();
            case TestCora.ID: return TestCora.numberOfFeatures();
            default: throw new IllegalArgumentException(formatWithLocale("Unknown Cora dataset '%s'.", datasetId));
        }
    }

    @Override
    public void generate(Path datasetDir, DbCreator dbCreator) {
        GdsGraphDatabaseAPI db = null;
        try {
            Files.createDirectories(datasetDir);
            db = dbCreator.createEmbeddedDatabase(datasetDir);
            loadNodes(db, coraNodesFile());
            loadRelationships(db);
        } catch (IOException e) {
            throw new RuntimeException("Could not generate dataset: " + getId(), e);
        } finally {
            if (db != null) {
                db.shutdown();
            }
        }
    }

    void loadNodes(GraphDatabaseService db, String nodeFile) throws IOException {
        try (Transaction tx = db.beginTx()) {
            ConstraintCreator paper = tx.schema().constraintFor(PAPER_LABEL);
            paper.assertPropertyIsUnique("extId");
            tx.commit();
        }
        InputStream nodesData = getClass().getClassLoader().getResourceAsStream(nodeFile);
        try (
            Transaction tx = db.beginTx();
            BufferedReader reader = new BufferedReader(new InputStreamReader(nodesData, StandardCharsets.UTF_8))
        ) {
            reader.lines()
                .map(line -> line.split(","))
                .forEach(entries -> {
                    long nodeExtId = Long.parseLong(entries[0]);
                    String subject = entries[1];
                    Node node = tx.createNode(PAPER_LABEL);
                    node.setProperty(EXT_ID_NODE_PROPERTY, nodeExtId);
                    node.setProperty(SUBJECT_NODE_PROPERTY, subject);
                    // any remaining entries are considered word features
                    for (int i = 2; i < entries.length; i++) {
                        int value = Integer.parseInt(entries[i]);
                        if (value == 1) {
                            node.setProperty("w" + (i - 2), value);
                        }
                    }
                });
            tx.commit();
        }

        // The Cora dataset contains 1400+ features. Each feature is 0 or 1. Storing 1400+ node properties is slow.
        // Therefore, we only store the 1s, the features that are present for a node.
        // One of the features is 0 for every node. By only storing properties on the nodes on which they are set,
        // we will miss this property, creating a gap in the sequence of property names. To work around this, we
        // create a dummy node with this property. When loading, we include this node so that the property type is known.
        // When running we filter to exclude it. This allows us to use all of the features present in the Cora
        // dataset, with a default value of 0, and not fail when we hit the gap because this one property key doesn't
        // exist.
        db.executeTransactionally("CREATE (f:MagicNode) SET f.w444 = 0");
    }

    private void loadRelationships(GraphDatabaseService db, RelationshipType relationshipType, String relationshipsFile) throws IOException {
        InputStream relationshipsData = getClass().getClassLoader().getResourceAsStream(relationshipsFile);
        try (
            Transaction tx = db.beginTx();
            BufferedReader reader = new BufferedReader(new InputStreamReader(relationshipsData, StandardCharsets.UTF_8))
        ) {
            reader.lines()
                .map(line -> line.split(","))
                .forEach(pair -> {
                    long sourceExtId = Long.parseLong(pair[0]);
                    long targetExtId = Long.parseLong(pair[1]);
                    Node source = tx.findNode(PAPER_LABEL, "extId", sourceExtId);
                    Node target = tx.findNode(PAPER_LABEL, "extId", targetExtId);
                    source.createRelationshipTo(target, relationshipType);
                });
            tx.commit();
        }
    }

    private void loadRelationships(GraphDatabaseService db) throws IOException {
        loadRelationships(db, TRAIN_RELATIONSHIP_TYPE, trainRelationshipsFile());
        loadRelationships(db, TEST_RELATIONSHIP_TYPE, testRelationshipsFile());
        loadRelationships(db, CITES_RELATIONSHIP_TYPE, citesRelationshipsFile());
    }
}
