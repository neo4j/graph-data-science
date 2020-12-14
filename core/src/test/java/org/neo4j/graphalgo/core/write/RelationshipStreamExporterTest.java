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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class RelationshipStreamExporterTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { id: 0 })" +
        ", (b { id: 1 })" +
        ", (c { id: 2 })" +
        ", (d { id: 3 })";

    private Graph graph;

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
        graph = new StoreLoaderBuilder().api(db).build().graphStore().getUnion();
    }

    @Test
    void exportScalar() {
        var exportRelationships = List.of(
            relationship(0, 1, Values.longValue(42L), Values.doubleValue(1332)),
            relationship(0, 2, Values.longValue(43L), Values.doubleValue(1333)),
            relationship(1, 0, Values.longValue(44L), Values.doubleValue(1334)),
            relationship(2, 2, Values.longValue(45L), Values.doubleValue(1335)),
            relationship(2, 3, Values.longValue(46L), Values.doubleValue(1336)),
            relationship(2, 3, Values.longValue(47L), Values.doubleValue(1337))
        );

        var exporter = RelationshipStreamExporter
            .builder(db, graph, exportRelationships.stream(), TerminationFlag.RUNNING_TRUE)
            .build();

        var relationshipType = "FOOBAR";
        var longKey = "x";
        var doubleKey = "y";
        var relationshipsWritten = exporter.write(relationshipType, longKey, doubleKey);

        assertEquals(exportRelationships.size(), relationshipsWritten);

        var exportedGraph = new StoreLoaderBuilder().api(db)
            .addRelationshipType(relationshipType)
            .addRelationshipProperty(longKey, longKey, DefaultValue.forLong(), Aggregation.NONE)
            .addRelationshipProperty(doubleKey, doubleKey, DefaultValue.forDouble(), Aggregation.NONE)
            .build()
            .graphStore()
            .getUnion();

        assertGraphEquals(
            fromGdl("(a), (b), (c), (d), " +
                    "(a)-[:FOOBAR { w: 42 }]->(b)" +
                    "(a)-[:FOOBAR { w: 43 }]->(c)" +
                    "(b)-[:FOOBAR { w: 44 }]->(a)" +
                    "(c)-[:FOOBAR { w: 45 }]->(c)" +
                    "(c)-[:FOOBAR { w: 46 }]->(d)" +
                    "(c)-[:FOOBAR { w: 47 }]->(d)" +
                    "(a)-[:FOOBAR { w: 1332.0 }]->(b)" +
                    "(a)-[:FOOBAR { w: 1333.0 }]->(c)" +
                    "(b)-[:FOOBAR { w: 1334.0 }]->(a)" +
                    "(c)-[:FOOBAR { w: 1335.0 }]->(c)" +
                    "(c)-[:FOOBAR { w: 1336.0 }]->(d)" +
                    "(c)-[:FOOBAR { w: 1337.0 }]->(d)"
            ),
            exportedGraph
        );
    }

    @Test
    void exportArray() {
        var exportRelationships = List.of(
            relationship(0, 1, Values.longArray(new long[]{1, 3, 3, 2}), Values.doubleArray(new double[]{4, 2})),
            relationship(0, 2, Values.longArray(new long[]{1, 3, 3, 3}), Values.doubleArray(new double[]{4, 3})),
            relationship(1, 0, Values.longArray(new long[]{1, 3, 3, 4}), Values.doubleArray(new double[]{4, 4})),
            relationship(2, 2, Values.longArray(new long[]{1, 3, 3, 5}), Values.doubleArray(new double[]{4, 5})),
            relationship(2, 3, Values.longArray(new long[]{1, 3, 3, 6}), Values.doubleArray(new double[]{4, 6})),
            relationship(2, 3, Values.longArray(new long[]{1, 3, 3, 7}), Values.doubleArray(new double[]{4, 7}))
        );

        var exporter = RelationshipStreamExporter
            .builder(db, graph, exportRelationships.stream(), TerminationFlag.RUNNING_TRUE)
            .build();

        var relationshipType = "FOOBAR";
        var longArrayKey = "x";
        var doubleArrayKey = "y";
        var relationshipsWritten = exporter.write(relationshipType, longArrayKey, doubleArrayKey);

        assertEquals(exportRelationships.size(), relationshipsWritten);

        //@formatter:off
        var query = "MATCH (a {id: $idA})-[r]->(b {id: $idB}) RETURN r.x AS x, r.y AS y";
        assertCypherResult(query, Map.of("idA", 0, "idB", 1), List.of(Map.of("x", new long[]{1, 3, 3, 2}, "y", new double[]{4, 2})));
        assertCypherResult(query, Map.of("idA", 0, "idB", 2), List.of(Map.of("x", new long[]{1, 3, 3, 3}, "y", new double[]{4, 3})));
        assertCypherResult(query, Map.of("idA", 1, "idB", 0), List.of(Map.of("x", new long[]{1, 3, 3, 4}, "y", new double[]{4, 4})));
        assertCypherResult(query, Map.of("idA", 2, "idB", 2), List.of(Map.of("x", new long[]{1, 3, 3, 5}, "y", new double[]{4, 5})));
        assertCypherResult(query, Map.of("idA", 2, "idB", 3),
            List.of(
                Map.of("x", new long[]{1, 3, 3, 6}, "y", new double[]{4, 6}),
                Map.of("x", new long[]{1, 3, 3, 7}, "y", new double[]{4, 7})
            )
        );
        //@formatter:on
    }

    @Test
    void exportExceedsBufferSize() {
        int nodeCount = 4;
        var batchSize = 10;
        // enforce writing non-full buffer
        var relationshipCount = 105;

        var rand = new Random();

        var relationshipStream = IntStream
            .range(0, relationshipCount)
            .mapToObj(ignored -> relationship(rand.nextInt(nodeCount), rand.nextInt(nodeCount)));

        var exporter = RelationshipStreamExporter
            .builder(db, graph, relationshipStream, TerminationFlag.RUNNING_TRUE)
            .withBatchSize(batchSize)
            .build();

        var relationshipsWritten = exporter.write("FOOBAR");

        assertEquals(relationshipCount, relationshipsWritten);
        assertCypherResult(
            "MATCH ()-[r]->() RETURN count(r) AS count",
            List.of(Map.of("count", (long) relationshipCount))
        );
    }

    @Test
    void exportEmptyStream() {
        var exporter = RelationshipStreamExporter
            .builder(db, graph, Stream.empty(), TerminationFlag.RUNNING_TRUE)
            .build();

        var relationshipsWritten = exporter.write("FOOBAR");

        assertEquals(0, relationshipsWritten);
    }

    @Test
    void logProgress() {
        int nodeCount = 4;
        var batchSize = 25;
        // enforce writing non-full buffer
        var relationshipCount = 105;

        var rand = new Random();

        var log = new TestLog();

        var relationshipStream = IntStream
            .range(0, relationshipCount)
            .mapToObj(ignored -> relationship(rand.nextInt(nodeCount), rand.nextInt(nodeCount)));

        var exporter = RelationshipStreamExporter
            .builder(db, graph, relationshipStream, TerminationFlag.RUNNING_TRUE)
            .withBatchSize(batchSize)
            .withLog(log)
            .build();

        var relationshipsWritten = exporter.write("FOOBAR");

        assertEquals(relationshipCount, relationshipsWritten);

        var messages = log.getMessages("info");
        assertEquals(7, messages.size());

        assertThat(messages.get(0)).contains("WriteRelationshipStream :: Start");
        assertThat(messages.get(1)).contains("WriteRelationshipStream Wrote 25 relationships");
        assertThat(messages.get(2)).contains("WriteRelationshipStream Wrote 50 relationships");
        assertThat(messages.get(3)).contains("WriteRelationshipStream Wrote 75 relationships");
        assertThat(messages.get(4)).contains("WriteRelationshipStream Wrote 100 relationships");
        assertThat(messages.get(5)).contains("WriteRelationshipStream Wrote 105 relationships");
        assertThat(messages.get(6)).contains("WriteRelationshipStream :: Finished");
    }

    @Test
    void throwsForParallelStreams() {
        var relationshipStream = IntStream
            .range(0, 10)
            .mapToObj(ignored -> relationship(0, 0))
            .parallel();

        assertThatThrownBy(() -> RelationshipStreamExporter.builder(db, graph, relationshipStream, TerminationFlag.RUNNING_TRUE).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("supports only sequential streams");
    }

    RelationshipStreamExporter.Relationship relationship(int sourceProperty, int targetProperty, Value... values) {
        return ImmutableRelationship.of(
            graph.toMappedNodeId(TestSupport.nodeIdByProperty(db, sourceProperty)),
            graph.toMappedNodeId(TestSupport.nodeIdByProperty(db, targetProperty)),
            values
        );
    }
}
