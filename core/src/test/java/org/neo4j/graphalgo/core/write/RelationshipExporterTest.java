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

package org.neo4j.graphalgo.core.write;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.core.utils.TerminationFlag.RUNNING_TRUE;

class RelationshipExporterTest {

    private static final String NODE_QUERY_PART =
        "CREATE"+
        "  (a)"+
        ", (b)"+
        ", (c)";

    private static final String RELS_QUERY_PART =
        ", (a)-[:BARFOO {weight: 4.2}]->(b)" +
        ", (a)-[:BARFOO {weight: 1.0}]->(a)" +
        ", (a)-[:BARFOO {weight: 2.3}]->(c)" +
        ", (b)-[:BARFOO]->(c)";

    private static final String DB_CYPHER = NODE_QUERY_PART;

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, DB_CYPHER);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void exportRelationships() {
        RelationshipExporter build = setupExportTest();
        build.write("FOOBAR", "weight", 0.0);
        validateWrittenGraph();
    }

    @Test
    void exportRelationshipsWithAfterWriteConsumer() {
        RelationshipExporter build = setupExportTest();
        MutableInt count = new MutableInt();
        build.write("FOOBAR", "weight", 0.0, (sourceNodeId, targetNodeId, property) -> {
            count.increment();
            return true;
        });
        Assertions.assertEquals(4, count.getValue());
        validateWrittenGraph();
    }

    private RelationshipExporter setupExportTest() {
        // create graph to export
        GraphDatabaseAPI fromDb = TestDatabaseCreator.createTestDatabase();
        fromDb.execute(NODE_QUERY_PART + RELS_QUERY_PART);
        Graph fromGraph = new GraphLoader(fromDb)
            .withAnyLabel()
            .withRelationshipType("BARFOO")
            .withRelationshipProperties(PropertyMapping.of("weight", 2.0))
            .load(HugeGraphFactory.class);

        // export into new database
        return RelationshipExporter
            .of(db, fromGraph, Direction.OUTGOING, RUNNING_TRUE)
            .build();
    }

    private void validateWrittenGraph() {
        Graph actual = new GraphLoader(db)
            .withAnyLabel()
            .withRelationshipType("FOOBAR")
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .load(HugeGraphFactory.class);

        assertGraphEquals(
            fromGdl(
                "(a)-[{w: 4.2}]->(b)," +
                "(a)-[{w: 1.0}]->(a)," +
                "(a)-[{w: 2.3}]->(c)," +
                "(b)-[{w: 2.0}]->(c)"),
            actual
        );
    }
}
