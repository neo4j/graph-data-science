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
package org.neo4j.gds.datasets;

import org.neo4j.gds.compat.GdsGraphDatabaseAPI;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.nio.file.Path;

import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;

public final class FakeLdbcDataset extends Dataset {

    public static final String NAME = "fake_ldbc";

    private FakeLdbcDataset(String id) {
        super(id);
    }

    static final FakeLdbcDataset INSTANCE = new FakeLdbcDataset(NAME);

    @Override
    protected void generate(Path datasetDir, DbCreator dbCreator) {
        Label[] labels = {
            Label.label("City"),
            Label.label("Comment"),
            Label.label("Company"),
            Label.label("Continent"),
            Label.label("Country"),
            Label.label("Forum"),
            Label.label("Message"),
            Label.label("Person"),
            Label.label("Post"),
            Label.label("Tag"),
            Label.label("TagClass"),
            Label.label("University"),
        };
        RelationshipType[] types = {
            RelationshipType.withName("COMMENT_HAS_CREATOR"),
            RelationshipType.withName("COMMENT_HAS_TAG"),
            RelationshipType.withName("COMMENT_IS_LOCATED_IN"),
            RelationshipType.withName("CONTAINER_OF"),
            RelationshipType.withName("FORUM_HAS_TAG"),
            RelationshipType.withName("HAS_INTEREST"),
            RelationshipType.withName("HAS_MEMBER"),
            RelationshipType.withName("HAS_MODERATOR"),
            RelationshipType.withName("HAS_TYPE"),
            RelationshipType.withName("IS_PART_OF"),
            RelationshipType.withName("IS_SUBCLASS_OF"),
            RelationshipType.withName("KNOWS"),
            RelationshipType.withName("LIKES_COMMENT"),
            RelationshipType.withName("LIKES_POST"),
            RelationshipType.withName("ORGANISATION_IS_LOCATED_IN"),
            RelationshipType.withName("PERSON_IS_LOCATED_IN"),
            RelationshipType.withName("POST_HAS_CREATOR"),
            RelationshipType.withName("POST_HAS_TAG"),
            RelationshipType.withName("POST_IS_LOCATED_IN"),
            RelationshipType.withName("REPLY_OF_COMMENT"),
            RelationshipType.withName("REPLY_OF_POST"),
            RelationshipType.withName("STUDY_AT"),
            RelationshipType.withName("WORKS_AT"),
        };

        GdsGraphDatabaseAPI db = dbCreator.createEmbeddedDatabase(datasetDir);
        runInTransaction(db, tx -> {
            Node nodeA = tx.createNode(labels);
            nodeA.setProperty("creationDate", 42);
            Node nodeB = tx.createNode(labels);
            nodeB.setProperty("creationDate", 1337);
            for (RelationshipType type : types) {
                nodeA.createRelationshipTo(nodeB, type);
                nodeB.createRelationshipTo(nodeA, type);
            }
        });
        db.shutdown();
    }
}
