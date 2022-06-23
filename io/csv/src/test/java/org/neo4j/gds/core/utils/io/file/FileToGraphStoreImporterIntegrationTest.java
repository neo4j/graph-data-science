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
package org.neo4j.gds.core.utils.io.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.utils.io.file.csv.CsvToGraphStoreImporter;
import org.neo4j.gds.core.utils.io.file.csv.GraphStoreToCsvExporter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

@GdlExtension
class FileToGraphStoreImporterIntegrationTest {

    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (a:A:B { prop1: 0, prop2: 42, prop3: [1L, 3L, 3L, 7L]})" +
        ", (b:A:B { prop1: 1, prop2: 43})" +
        ", (c:A:C { prop1: 2, prop2: 44, prop3: [1L, 9L, 8L, 4L] })" +
        ", (d:B { prop1: 3 })" +
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
    @ValueSource(booleans = { true, false })
    void shouldImportCapabilities(boolean canWriteToDatabase) {
        var graphStoreWithCapabilities = GdlFactory.builder()
            .gdlGraph("()-[]->()")
            .graphCapabilities(ImmutableStaticCapabilities.of(canWriteToDatabase))
            .build()
            .build();

        GraphStoreToCsvExporter.create(graphStoreWithCapabilities, exportConfig(1), graphLocation).run();

        var importer = new CsvToGraphStoreImporter(1, graphLocation, Neo4jProxy.testLog(), EmptyTaskRegistryFactory.INSTANCE);
        var userGraphStore = importer.run();

        var importedGraphStore = userGraphStore.graphStore();

        assertThat(importedGraphStore.capabilities())
            .usingRecursiveComparison()
            .isEqualTo(ImmutableStaticCapabilities.of(canWriteToDatabase));
    }

    private GraphStoreToFileExporterConfig exportConfig(int concurrency) {
        return ImmutableGraphStoreToFileExporterConfig.builder()
            .exportName("my-export")
            .writeConcurrency(concurrency)
            .includeMetaData(true)
            .build();
    }
}
