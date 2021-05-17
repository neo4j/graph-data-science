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
package org.neo4j.graphalgo.core.write;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.gdl.GdlFactory;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.values.storable.Values;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;
import static org.neo4j.graphalgo.core.utils.TerminationFlag.RUNNING_TRUE;

class RelationshipExporterTest extends BaseTest {

    private static final String NODE_QUERY_PART =
        "CREATE" +
        "  (a)" +
        ", (b)" +
        ", (c)" +
        ", (d)";

    private static final String RELS_QUERY_PART =
        ", (a)-[:BARFOO {weight: 4.2, weight2: 4}]->(b)" +
        ", (a)-[:BARFOO {weight: 1.0, weight2: 5}]->(a)" +
        ", (a)-[:BARFOO {weight: 2.3, weight2: 6}]->(c)" +
        ", (b)-[:BARFOO]->(c)" +
        ", (c)-[:THISISNOTTHETYPEYOUARELOOKINGFOR]->(d)";

    private static final String DB_CYPHER = NODE_QUERY_PART;
    private static final double PROPERTY_VALUE_IF_MISSING = 2.0;
    private static final double PROPERTY_VALUE_IF_NOT_WRITTEN = 1337.0;

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @Test
    void doesNotExportWhenNotAllowed() {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .graphName("foo")
            .build()
            .graph();

        var secureTransaction = TestSupport.fullAccessTransaction(db).withRestrictedAccess(AccessMode.Static.READ);
        var exporter = RelationshipExporter.of(secureTransaction, graph, RUNNING_TRUE).build();

        assertThatExceptionOfType(AuthorizationViolationException.class)
            .isThrownBy(() -> exporter.write("OUT_TYPE"));
    }

    @Test
    void exportRelationships() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ true);
        exporter.write("FOOBAR", "weight");
        validateWrittenGraph();
    }

    @Test
    void exportRelationshipsWithLongProperties() {
        clearDb();
        runQuery(NODE_QUERY_PART + RELS_QUERY_PART);

        GraphStore graphStore = new StoreLoaderBuilder().api(db)
            .putRelationshipProjectionsWithIdentifier(
                "NEW_REL",
                RelationshipProjection.of("BARFOO", Orientation.NATURAL)
            )
            .addRelationshipProperty("newWeight", "weight2", DefaultValue.of(0), Aggregation.NONE)
            .build()
            .graphStore();

        RelationshipExporter
            .of(
                TestSupport.fullAccessTransaction(db),
                graphStore.getGraph(RelationshipType.of("NEW_REL"), Optional.of("newWeight")),
                RUNNING_TRUE
            )
            .withRelationPropertyTranslator(relProperty -> Values.longValue((long) relProperty))
            .build()
            .write("NEW_REL", "newWeight");

        runQueryWithRowConsumer(db,
            "MATCH ()-[r:NEW_REL]->() RETURN r.newWeight AS newWeight",
            row -> assertThat(row.get("newWeight")).isInstanceOf(Long.class));
    }

    @Test
    void exportRelationshipsWithoutProperties() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ false);
        exporter.write("FOOBAR");
        validateWrittenGraphWithoutProperties();

        runQueryWithRowConsumer(
            db,
            "MATCH ()-[r:FOOBAR]->() RETURN sum(size(keys(r))) AS keyCount",
            Collections.emptyMap(),
            row -> assertEquals(0L, row.getNumber("keyCount").longValue())
        );
    }

    @Test
    void exportRelationshipsExcludePresentProperties() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ true);
        exporter.write("FOOBAR");
        validateWrittenGraphWithoutProperties();

        runQueryWithRowConsumer(
            db,
            "MATCH ()-[r:FOOBAR]->() RETURN sum(size(keys(r))) AS keyCount",
            Collections.emptyMap(),
            row -> assertEquals(0L, row.getNumber("keyCount").longValue())
        );
    }

    @Test
    void exportRelationshipsWithAfterWriteConsumer() {
        RelationshipExporter exporter = setupExportTest(/* includeProperties */ true);
        MutableInt count = new MutableInt();
        exporter.write("FOOBAR", "weight", (sourceNodeId, targetNodeId, property) -> {
            count.increment();
            return true;
        });
        assertEquals(4, count.getValue());
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
        assertEquals(4, count.getValue());
        validateWrittenGraphWithoutProperties();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void progressLogging(boolean parallel) {
        // given a graph of 20 rels
        // this abuses id mapping
        Graph graph = GdlFactory.of("(a)-[:T]->(b),".repeat(20)).build().graphStore().getUnion();

        // with a rel exporter
        var log = new TestLog();
        var exporterBuilder = RelationshipExporter
            .of(TestSupport.fullAccessTransaction(db), graph, TerminationFlag.RUNNING_TRUE)
            .withLog(log);
        if (parallel) {
            exporterBuilder = exporterBuilder.parallel(Pools.DEFAULT, 4);
        }
        var exporter = exporterBuilder.build();

        // when writing properties
        exporter.write("T");

        // then assert messages
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "WriteRelationships :: Start",
                "WriteRelationships 5%",
                "WriteRelationships 10%",
                "WriteRelationships 15%",
                "WriteRelationships 20%",
                "WriteRelationships 25%",
                "WriteRelationships 30%",
                "WriteRelationships 35%",
                "WriteRelationships 40%",
                "WriteRelationships 45%",
                "WriteRelationships 50%",
                "WriteRelationships 55%",
                "WriteRelationships 60%",
                "WriteRelationships 65%",
                "WriteRelationships 70%",
                "WriteRelationships 75%",
                "WriteRelationships 80%",
                "WriteRelationships 85%",
                "WriteRelationships 90%",
                "WriteRelationships 95%",
                "WriteRelationships 100%",
                "WriteRelationships :: Finished"
            );
    }

    private RelationshipExporter setupExportTest(boolean includeProperties) {
        // create graph to export
        clearDb();
        runQuery(NODE_QUERY_PART + RELS_QUERY_PART);

        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder()
            .api(db)
            .addRelationshipType("BARFOO");
        if (includeProperties) {
            storeLoaderBuilder.addRelationshipProperty(PropertyMapping.of("weight", PROPERTY_VALUE_IF_MISSING));
        }

        Graph fromGraph = storeLoaderBuilder
            .build()
            .graph();


        // export into new database
        return RelationshipExporter
            .of(TestSupport.fullAccessTransaction(db), fromGraph, RUNNING_TRUE)
            .build();
    }

    private void validateWrittenGraph() {
        Graph actual = loadWrittenGraph(true);
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
        Graph actual = loadWrittenGraph(false);
        assertGraphEquals(
            fromGdl(
                "(a)-->(b)," +
                "(a)-->(a)," +
                "(a)-->(c)," +
                "(b)-->(c)," +
                "(d)"),
            actual
        );
    }

    private Graph loadWrittenGraph(boolean loadRelProperty) {
        StoreLoaderBuilder loader = new StoreLoaderBuilder()
            .api(db)
            .addRelationshipType("FOOBAR");
        if (loadRelProperty) {
            loader.addRelationshipProperty(PropertyMapping.of("weight", PROPERTY_VALUE_IF_NOT_WRITTEN));
        }
        return loader.build().graph();
    }
}
