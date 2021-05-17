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
package org.neo4j.graphalgo.core.utils.export.file;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.loading.BitIdMap;
import org.neo4j.graphalgo.core.loading.CSRGraphStoreUtil;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.TestMethodRunner;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.values.storable.Values;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

@GdlExtension
class CsvToGraphStoreExporterIntegrationTest {

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

        GraphStoreToFileExporter.csv(graphStore, config(concurrency), graphLocation).run(AllocationTracker.empty());

        var importer = CsvToGraphStoreExporter.create(config(concurrency), graphLocation, new TestLog());
        importer.run(AllocationTracker.empty());

        var importedGraphStore = importer.userGraphStore().graphStore();
        var importedGraph = importedGraphStore.getUnion();
        assertGraphEquals(graph, importedGraph);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void withBitIdMap(int concurrency) {
        TestMethodRunner.runWithBitIdMap(() -> {
            var nodeSchema = NodeSchema.builder()
                .addProperty(NodeLabel.of("A"), "prop1", ValueType.LONG)
                .addProperty(NodeLabel.of("B"), "prop2", ValueType.DOUBLE)
                .build();
            var nodesBuilder = GraphFactory.initNodesBuilder()
                .nodeSchema(nodeSchema)
                .maxOriginalId(1337L)
                .nodeCount(3L)
                .hasDisjointPartitions(true)
                .tracker(AllocationTracker.empty())
                .concurrency(concurrency)
                .build();

            nodesBuilder.addNode(1335L, Map.of("prop1", Values.longValue(42L)), NodeLabel.of("A"));
            nodesBuilder.addNode(1336L, Map.of("prop1", Values.longValue(43L)), NodeLabel.of("A"));
            nodesBuilder.addNode(1337L, Map.of("prop2", Values.doubleValue(13.37D)), NodeLabel.of("B"));

            var nodeMapping = nodesBuilder.build().nodeMapping();

            var graphStore = graphStoreFromNodeMapping(nodeMapping);
            var expectedGraph = graphStore.getUnion();

            GraphStoreToFileExporter.csv(graphStore, config(concurrency), graphLocation).run(AllocationTracker.empty());

            var importer = CsvToGraphStoreExporter.create(config(concurrency), graphLocation, new TestLog());
            importer.run(AllocationTracker.empty());

            var importedGraphStore = importer.userGraphStore().graphStore();
            var importedGraph = importedGraphStore.getUnion();
            assertThat(importedGraphStore.nodes()).isInstanceOf(BitIdMap.class);
            assertGraphEquals(expectedGraph, importedGraph);
        });
    }

    private GraphStoreToFileExporterConfig config(int concurrency) {
        return ImmutableGraphStoreToFileExporterConfig.builder()
            .exportName("my-export")
            .writeConcurrency(concurrency)
            .includeMetaData(true)
            .build();
    }

    static GraphStore graphStoreFromNodeMapping(NodeMapping nodeMapping) {
        var relationships = GraphFactory.emptyRelationships(nodeMapping, AllocationTracker.empty());
        var hugeGraph = GraphFactory.create(nodeMapping, relationships, AllocationTracker.empty());
        return CSRGraphStoreUtil.createFromGraph(
            DatabaseIdFactory.from("Test", UUID.randomUUID()),
            hugeGraph,
            "REL",
            Optional.empty(),
            1,
            AllocationTracker.empty()
        );
    }

}
