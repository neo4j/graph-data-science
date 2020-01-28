/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.core.utils.TerminationFlag.RUNNING_TRUE;

class RelationshipExporterTest {

    private static final String NODE_QUERY_PART =
        "CREATE" +
        "  (a)" +
        ", (b)" +
        ", (c)" +
        ", (d)";

    private static final String RELS_QUERY_PART =
        ", (a)-[:BARFOO {weight: 4.2}]->(b)" +
        ", (a)-[:BARFOO {weight: 1.0}]->(a)" +
        ", (a)-[:BARFOO {weight: 2.3}]->(c)" +
        ", (b)-[:BARFOO]->(c)" +
        ", (c)-[:THISISNOTTHETYPEYOUARELOOKINGFOR]->(d)";

    private static final String DB_CYPHER = NODE_QUERY_PART;
    private static final double PROPERTY_VALUE_IF_MISSING = 2.0;
    private static final double PROPERTY_VALUE_IF_NOT_WRITTEN = 1337.0;

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
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ true);
        exporter.write("FOOBAR", "weight");
        validateWrittenGraph();
    }

    @Test
    void exportRelationshipsWithoutProperties() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ false);
        exporter.write("FOOBAR", "weight");
        validateWrittenGraphWithoutProperties();
    }

    @Test
    void exportRelationshipsWithAfterWriteConsumer() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ true);
        MutableInt count = new MutableInt();
        exporter.write("FOOBAR", "weight", (sourceNodeId, targetNodeId, property) -> {
            count.increment();
            return true;
        });
        Assertions.assertEquals(4, count.getValue());
        validateWrittenGraph();
    }

    @Test
    void exportRelationshipsWithAfterWriteConsumerAndNoProperties() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ false);
        MutableInt count = new MutableInt();
        exporter.write("FOOBAR", "weight", (sourceNodeId, targetNodeId, property) -> {
            count.increment();
            return true;
        });
        Assertions.assertEquals(4, count.getValue());
        validateWrittenGraphWithoutProperties();
    }

    private RelationshipExporter setupExportTest(boolean includeProperties) {
        // create graph to export
        GraphDatabaseAPI fromDb = TestDatabaseCreator.createTestDatabase();
        runQuery(fromDb, NODE_QUERY_PART + RELS_QUERY_PART);

        Graph fromGraph = new StoreLoaderBuilder()
            .api(fromDb)
            .loadAnyLabel()
            .addRelationshipType("BARFOO")
            .addRelationshipProperty(PropertyMapping.of("weight", PROPERTY_VALUE_IF_MISSING))
            .build()
            .graph(HugeGraphFactory.class);

        if (!includeProperties) {
            fromGraph = fromGraph.withoutRelationshipProperties();
        }

        // export into new database
        return RelationshipExporter
            .of(db, fromGraph, Direction.OUTGOING, RUNNING_TRUE)
            .build();
    }

    private void validateWrittenGraph() {
        Graph actual = loadWrittenGraph();
        assertGraphEquals(
            fromGdl(
                "(a)-[{w: 4.2}]->(b)," +
                "(a)-[{w: 1.0}]->(a)," +
                "(a)-[{w: 2.3}]->(c)," +
                "(b)-[{w: " + PROPERTY_VALUE_IF_MISSING + "}]->(c)," +
                "(d)"),
            actual
        );
    }

    private void validateWrittenGraphWithoutProperties() {
        Graph actual = loadWrittenGraph();
        assertGraphEquals(
            fromGdl(
                "(a)-[{w: " + PROPERTY_VALUE_IF_NOT_WRITTEN + "}]->(b)," +
                "(a)-[{w: " + PROPERTY_VALUE_IF_NOT_WRITTEN + "}]->(a)," +
                "(a)-[{w: " + PROPERTY_VALUE_IF_NOT_WRITTEN + "}]->(c)," +
                "(b)-[{w: " + PROPERTY_VALUE_IF_NOT_WRITTEN + "}]->(c)," +
                "(d)"),
            actual
        );
    }

    private Graph loadWrittenGraph() {
        return new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .addRelationshipType("FOOBAR")
            .addRelationshipProperty(PropertyMapping.of("weight", PROPERTY_VALUE_IF_NOT_WRITTEN))
            .build()
            .graph(HugeGraphFactory.class);
    }
}
