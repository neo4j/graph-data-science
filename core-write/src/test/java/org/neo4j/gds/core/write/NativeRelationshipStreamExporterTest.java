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
package org.neo4j.gds.core.write;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.core.utils.TerminationFlag.RUNNING_TRUE;

class NativeRelationshipStreamExporterTest extends BaseTest {

    private static final List<String> nodeVariables = List.of("a", "b", "c", "d");

    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE (" + String.join("), (", nodeVariables) + ")";

    private Graph graph;

    @BeforeEach
    void setup() {
        graph = new StoreLoaderBuilder().databaseService(db).build().graphStore().getUnion();
    }

    @Test
    void doesNotExportWhenNotAllowed() {
        var exportRelationships = List.of(
            relationship("a", "b", Values.longValue(42L), Values.doubleValue(1332)),
            relationship("c", "d", Values.longValue(47L), Values.doubleValue(1337))
        );

        var secureTransaction = TestSupport.fullAccessTransaction(db).withRestrictedAccess(AccessMode.Static.READ);
        var exporter = NativeRelationshipStreamExporter
            .builder(secureTransaction, graph, exportRelationships.stream(), RUNNING_TRUE)
            .build();

        assertThatExceptionOfType(AuthorizationViolationException.class)
            .isThrownBy(() -> exporter.write("OUT_TYPE", List.of(), List.of()));
    }

    @Test
    void exportScalar() {
        var exportRelationships = List.of(
            relationship("a", "b", Values.longValue(42L), Values.doubleValue(1332)),
            relationship("a", "c", Values.longValue(43L), Values.doubleValue(1333)),
            relationship("b", "a", Values.longValue(44L), Values.doubleValue(1334)),
            relationship("c", "c", Values.longValue(45L), Values.doubleValue(1335)),
            relationship("c", "d", Values.longValue(46L), Values.doubleValue(1336)),
            relationship("c", "d", Values.longValue(47L), Values.doubleValue(1337))
        );

        var exporter = NativeRelationshipStreamExporter
            .builder(
                TestSupport.fullAccessTransaction(db),
                graph,
                exportRelationships.stream(),
                TerminationFlag.RUNNING_TRUE
            )
            .build();

        var relationshipType = "FOOBAR";
        var longKey = "x";
        var doubleKey = "y";
        var relationshipsWritten = exporter.write(relationshipType, List.of(longKey, doubleKey), List.of(ValueType.LONG, ValueType.DOUBLE));

        assertEquals(exportRelationships.size(), relationshipsWritten);

        var exportedGraph = new StoreLoaderBuilder().databaseService(db)
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
            relationship("a", "b", Values.longArray(new long[]{1, 3, 3, 2}), Values.doubleArray(new double[]{4, 2})),
            relationship("a", "c", Values.longArray(new long[]{1, 3, 3, 3}), Values.doubleArray(new double[]{4, 3})),
            relationship("b", "a", Values.longArray(new long[]{1, 3, 3, 4}), Values.doubleArray(new double[]{4, 4})),
            relationship("c", "c", Values.longArray(new long[]{1, 3, 3, 5}), Values.doubleArray(new double[]{4, 5})),
            relationship("c", "d", Values.longArray(new long[]{1, 3, 3, 6}), Values.doubleArray(new double[]{4, 6})),
            relationship("c", "d", Values.longArray(new long[]{1, 3, 3, 7}), Values.doubleArray(new double[]{4, 7}))
        );

        var exporter = NativeRelationshipStreamExporter
            .builder(
                TestSupport.fullAccessTransaction(db),
                graph,
                exportRelationships.stream(),
                TerminationFlag.RUNNING_TRUE
            )
            .build();

        var relationshipType = "FOOBAR";
        var longArrayKey = "x";
        var doubleArrayKey = "y";
        var relationshipsWritten = exporter.write(relationshipType, List.of(longArrayKey, doubleArrayKey), List.of(ValueType.LONG_ARRAY, ValueType.DOUBLE_ARRAY));

        assertEquals(exportRelationships.size(), relationshipsWritten);

        var idA =  idFunction.of("a");
        var idB =  idFunction.of("b");
        var idC =  idFunction.of("c");
        var idD =  idFunction.of("d");

        //@formatter:off
        var query = "MATCH (a)-[r:FOOBAR]->(b) WHERE id(a) = $idA AND id(b) = $idB RETURN r.x AS x, r.y AS y ORDER BY x, y";
        assertCypherResult(query, Map.of("idA", idA, "idB", idB), List.of(Map.of("x", new long[]{1, 3, 3, 2}, "y", new double[]{4, 2})));
        assertCypherResult(query, Map.of("idA", idA, "idB", idC), List.of(Map.of("x", new long[]{1, 3, 3, 3}, "y", new double[]{4, 3})));
        assertCypherResult(query, Map.of("idA", idB, "idB", idA), List.of(Map.of("x", new long[]{1, 3, 3, 4}, "y", new double[]{4, 4})));
        assertCypherResult(query, Map.of("idA", idC, "idB", idC), List.of(Map.of("x", new long[]{1, 3, 3, 5}, "y", new double[]{4, 5})));
        assertCypherResult(query, Map.of("idA", idC, "idB", idD),
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
            .mapToObj(ignored -> relationship(randomVariable(rand, nodeCount), randomVariable(rand, nodeCount)));

        var exporter = NativeRelationshipStreamExporter
            .builder(TestSupport.fullAccessTransaction(db), graph, relationshipStream, TerminationFlag.RUNNING_TRUE)
            .withBatchSize(batchSize)
            .build();

        var relationshipsWritten = exporter.write("FOOBAR", List.of(), List.of());

        assertEquals(relationshipCount, relationshipsWritten);
        assertCypherResult(
            "MATCH ()-[r]->() RETURN count(r) AS count",
            List.of(Map.of("count", (long) relationshipCount))
        );
    }

    @Test
    void exportEmptyStream() {
        var exporter = NativeRelationshipStreamExporter
            .builder(TestSupport.fullAccessTransaction(db), graph, Stream.empty(), TerminationFlag.RUNNING_TRUE)
            .build();

        var relationshipsWritten = exporter.write("FOOBAR", List.of(), List.of());

        assertEquals(0, relationshipsWritten);
    }

    @Test
    void logProgress() {
        int nodeCount = 4;
        var batchSize = 25;
        // enforce writing non-full buffer
        var relationshipCount = 105;

        var rand = new Random();

        var log = Neo4jProxy.testLog();
        var progressTracker = new TaskProgressTracker(
            RelationshipStreamExporter.baseTask("OpName"),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        var relationshipStream = IntStream
            .range(0, relationshipCount)
            .mapToObj(ignored -> relationship(randomVariable(rand, nodeCount), randomVariable(rand, nodeCount)));

        var exporter = NativeRelationshipStreamExporter
            .builder(TestSupport.fullAccessTransaction(db), graph, relationshipStream, TerminationFlag.RUNNING_TRUE)
            .withBatchSize(batchSize)
            .withProgressTracker(progressTracker)
            .build();

        var relationshipsWritten = exporter.write("FOOBAR", List.of(), List.of());

        assertEquals(relationshipCount, relationshipsWritten);

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "OpName :: WriteRelationshipStream :: Start",
                "OpName :: WriteRelationshipStream has written 25 relationships",
                "OpName :: WriteRelationshipStream has written 50 relationships",
                "OpName :: WriteRelationshipStream has written 75 relationships",
                "OpName :: WriteRelationshipStream has written 100 relationships",
                "OpName :: WriteRelationshipStream has written 105 relationships",
                "OpName :: WriteRelationshipStream 100%",
                "OpName :: WriteRelationshipStream :: Finished"
            );
    }

    ExportedRelationship relationship(String sourceVariable, String targetVariable, Value... values) {
        return ImmutableExportedRelationship.of(
            graph.toMappedNodeId(idFunction.of(sourceVariable)),
            graph.toMappedNodeId(idFunction.of(targetVariable)),
            values
        );
    }

    private String randomVariable(Random rand, int limit) {
        return nodeVariables.get(rand.nextInt(limit));
    }
}
