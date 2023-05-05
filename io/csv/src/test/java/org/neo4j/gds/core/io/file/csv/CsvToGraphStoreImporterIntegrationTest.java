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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.graph.DoubleArrayGraphPropertyValues;
import org.neo4j.gds.api.properties.graph.LongGraphPropertyValues;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.io.file.GraphStoreToFileExporterConfig;
import org.neo4j.gds.core.io.file.ImmutableGraphStoreToFileExporterConfig;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

@GdlExtension
class CsvToGraphStoreImporterIntegrationTest {

    @GdlGraph
    private static final String GDL =
        "CREATE" +
        //                                      This triggers jackson wrapping the values in quotes
        "  (a:A:B { averylongpropertynamegreaterthantwentyfour: 0, prop2: 42, prop3: [0.30000001192092896D, 0.20000000298023224D]})" +
        ", (b:A:B { averylongpropertynamegreaterthantwentyfour: 1, prop2: 43})" +
        ", (c:A:C { averylongpropertynamegreaterthantwentyfour: 2, prop2: 44, prop3: [-0.04D] })" +
        ", (d:B { averylongpropertynamegreaterthantwentyfour: 3 })" +
        ", (a)-[:REL1 { prop1: 0, prop2: 42 }]->(a)" +
        ", (a)-[:REL1 { prop1: 1, prop2: 43 }]->(b)" +
        ", (b)-[:REL1 { prop1: 2, prop2: 44 }]->(a)" +
        ", (b)-[:REL2 { prop3: 3, prop4: 45 }]->(c)" +
        ", (c)-[:REL2 { prop3: 4, prop4: 46 }]->(d)" +
        ", (d)-[:REL2 { prop3: 5, prop4: 47 }]->(a)";

    @Inject
    GraphStore graphStore;

    @Inject
    Graph graph;

    @TempDir
    Path graphLocation;

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldImportProperties(int concurrency) {

        GraphStoreToCsvExporter.create(graphStore, exportConfig(concurrency), graphLocation).run();

        var importer = new CsvToGraphStoreImporter(concurrency, graphLocation, Neo4jProxy.testLog(), EmptyTaskRegistryFactory.INSTANCE);
        var userGraphStore = importer.run();

        var importedGraphStore = userGraphStore.graphStore();
        var importedGraph = importedGraphStore.getUnion();
        assertGraphEquals(graph, importedGraph);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldImportGraphStoreWithGraphProperties(int concurrency) {
        addLongGraphProperty();
        addDoubleArrayGraphProperty();
        addLongNamedGraphProperty();

        GraphStoreToCsvExporter.create(graphStore, exportConfig(concurrency), graphLocation).run();
        var importer = new CsvToGraphStoreImporter(concurrency, graphLocation, Neo4jProxy.testLog(), EmptyTaskRegistryFactory.INSTANCE);
        var userGraphStore = importer.run();
        var graphStore = userGraphStore.graphStore();

        assertThat(graphStore.graphPropertyKeys()).containsExactlyInAnyOrder(
            "longProp",
            "doubleArrayProp",
            "thisisaverylongnameintentionallytotriggerquoting"
        );

        var expectedLongValues = LongStream.range(0, 10_000).toArray();
        assertThat(graphStore.graphProperty("longProp").values().longValues().toArray())
            .containsExactlyInAnyOrder(expectedLongValues);

        var expectedDoubleArrayProperties = LongStream
            .range(0, 1337)
            .mapToObj(i -> new double[]{(double) i, 42.0})
            .collect(Collectors.toList())
            .toArray(new double[0][0]);
        assertThat(graphStore.graphProperty("doubleArrayProp").values().doubleArrayValues().collect(Collectors.toList()))
            .containsExactlyInAnyOrder(expectedDoubleArrayProperties);
    }

    @Test
    void shouldImportGraphWithNoLabels() {
        var graphStore = GdlFactory.of("()-[]->()").build();

        GraphStoreToCsvExporter.create(graphStore, exportConfig(4), graphLocation).run();

        var importer = new CsvToGraphStoreImporter(4, graphLocation, Neo4jProxy.testLog(), EmptyTaskRegistryFactory.INSTANCE);
        var userGraphStore = importer.run();

        var importedGraphStore = userGraphStore.graphStore();
        var importedGraph = importedGraphStore.getUnion();
        assertGraphEquals(graphStore.getUnion(), importedGraph);
    }

    @ParameterizedTest
    @EnumSource(WriteMode.class)
    void shouldImportCapabilities(WriteMode writeMode) {
        var graphStoreWithCapabilities = GdlFactory.builder()
            .gdlGraph("()-[]->()")
            .graphCapabilities(ImmutableStaticCapabilities.of(writeMode))
            .build()
            .build();

        GraphStoreToCsvExporter.create(graphStoreWithCapabilities, exportConfig(1), graphLocation).run();

        var importer = new CsvToGraphStoreImporter(1, graphLocation, Neo4jProxy.testLog(), EmptyTaskRegistryFactory.INSTANCE);
        var userGraphStore = importer.run();

        var importedGraphStore = userGraphStore.graphStore();

        assertThat(importedGraphStore.capabilities())
            .usingRecursiveComparison()
            .isEqualTo(ImmutableStaticCapabilities.of(writeMode));
    }

    private GraphStoreToFileExporterConfig exportConfig(int concurrency) {
        return ImmutableGraphStoreToFileExporterConfig.builder()
            .exportName("my-export")
            .writeConcurrency(concurrency)
            .includeMetaData(true)
            .build();
    }

    private void addDoubleArrayGraphProperty() {
        graphStore.addGraphProperty("doubleArrayProp", new DoubleArrayGraphPropertyValues() {
            @Override
            public Stream<double[]> doubleArrayValues() {
                return LongStream.range(0, 1337).mapToObj(i -> new double[]{ (double) i, 42.0 });
            }

            @Override
            public long valueCount() {
                return 1337;
            }
        });
    }

    private void addLongGraphProperty() {
        graphStore.addGraphProperty("longProp", new LongGraphPropertyValues() {
            @Override
            public LongStream longValues() {
                return LongStream.range(0, 10_000);
            }

            @Override
            public long valueCount() {
                return 10_000;
            }
        });
    }

    private void addLongNamedGraphProperty() {
        graphStore.addGraphProperty("thisisaverylongnameintentionallytotriggerquoting", new LongGraphPropertyValues() {
            @Override
            public LongStream longValues() {
                return LongStream.range(0, 10_000);
            }

            @Override
            public long valueCount() {
                return 10_000;
            }
        });
    }
}
