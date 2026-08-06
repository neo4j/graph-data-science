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
package org.neo4j.gds.core.io.file.csv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.DoubleVectorNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatVectorNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.VectorNodePropertyValues;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterParameters;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.logging.LoggerForProgressTracking;
import org.neo4j.gds.progress.registration.EmptyTaskRegistryFactory;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CsvVectorPropertyRoundTripTest {

    private static final String GDL = "CREATE (:A), (:A), (:A)";

    @TempDir
    Path graphLocation;

    @Test
    void aFloatVectorPropertySurvivesTheRoundTrip() throws IOException {
        var graphStore = graphStoreWithFloatVector();

        var importedGraphStore = roundTrip(graphStore);

        var imported = importedGraphStore.nodeProperty("embedding").values();
        assertThat(imported.valueType()).isEqualTo(ValueType.FLOAT_VECTOR);
        assertThat(imported).isInstanceOf(VectorNodePropertyValues.class);
        assertThat(((VectorNodePropertyValues) imported).vectorDimension()).isEqualTo(3);

        assertThat(imported.floatArrayValue(0)).containsExactly(0.5F, 1.5F, 2.5F);
        assertThat(imported.floatArrayValue(1)).containsExactly(1.5F, 2.5F, 3.5F);
        assertThat(imported.floatArrayValue(2)).containsExactly(2.5F, 3.5F, 4.5F);
    }

    @Test
    void aDoubleVectorPropertySurvivesTheRoundTrip() throws IOException {
        var graphStore = GdlFactory.of(GDL).build();

        var embeddings = new DoubleVectorNodePropertyValues() {
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{nodeId + 0.25D, nodeId + 1.25D};
            }

            @Override
            public int vectorDimension() {
                return 2;
            }

            @Override
            public long nodeCount() {
                return graphStore.nodeCount();
            }
        };

        graphStore.addNodeProperty(
            Set.of(NodeLabel.of("A")),
            "embedding",
            embeddings
        );

        var imported = roundTrip(graphStore).nodeProperty("embedding").values();

        assertThat(imported.valueType()).isEqualTo(ValueType.DOUBLE_VECTOR);
        assertThat(((VectorNodePropertyValues) imported).vectorDimension()).isEqualTo(2);
        assertThat(imported.doubleArrayValue(0)).containsExactly(0.25D, 1.25D);
        assertThat(headerLines()).anySatisfy(header ->
            assertThat(header).contains("embedding:double_vector(2)")
        );
    }

    @Test
    void theHeaderCarriesTheDimensionInTheTypeToken() throws IOException {
        roundTrip(graphStoreWithFloatVector());

        assertThat(headerLines()).anySatisfy(header ->
            assertThat(header).contains("embedding:float_vector(3)")
        );
    }

    @Test
    void theSchemaFileRecordsTheVectorType() throws IOException {
        roundTrip(graphStoreWithFloatVector());

        var nodeSchema = Files.readString(graphLocation.resolve("node-schema.csv"));
        assertThat(nodeSchema).contains("float_vector");
    }

    private GraphStore graphStoreWithFloatVector() {
        var graphStore = GdlFactory.of(GDL).build();

        var embeddings = new FloatVectorNodePropertyValues() {
            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{nodeId + 0.5F, nodeId + 1.5F, nodeId + 2.5F};
            }

            @Override
            public int vectorDimension() {
                return 3;
            }

            @Override
            public long nodeCount() {
                return graphStore.nodeCount();
            }
        };

        graphStore.addNodeProperty(
            Set.of(NodeLabel.of("A")),
            "embedding",
            embeddings
        );

        // the schema must agree with the values, otherwise export would write a plain array header
        assertThat(graphStore.nodeProperty("embedding").values().valueType()).isEqualTo(ValueType.FLOAT_VECTOR);

        return graphStore;
    }

    private GraphStore roundTrip(GraphStore graphStore) {
        var requestCorrelationId = PlainSimpleRequestCorrelationId.create();

        GraphStoreToCsvExporter.create(
            Log.noOpLog(),
            LoggerForProgressTracking.noOpLog(),
            graphStore,
            new GraphStoreToFileExporterParameters(
                "my-export",
                "",
                RelationshipType.ALL_RELATIONSHIPS,
                new Concurrency(1),
                10_000
            ),
            graphLocation,
            Optional.empty(),
            requestCorrelationId,
            new JobId(),
            TaskRegistryFactory.empty(),
            DefaultPool.INSTANCE
        ).run();

        return new CsvToGraphStoreImporter(
            new Concurrency(1),
            graphLocation,
            Log.noOpLog(),
            requestCorrelationId,
            EmptyTaskRegistryFactory.INSTANCE,
            new JobId()
        ).run().graphStore();
    }

    private Stream<String> headerLines() throws IOException {
        try (var files = Files.list(graphLocation)) {
            return files
                .filter(path -> path.getFileName().toString().contains("header"))
                .flatMap(path -> {
                    try {
                        return Files.readAllLines(path).stream();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList()
                .stream();
        }
    }
}
