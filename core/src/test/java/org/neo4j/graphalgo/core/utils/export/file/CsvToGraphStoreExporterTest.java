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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.PropertySchema;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class CsvToGraphStoreExporterTest {

    @Test
    void shouldImportProperties() throws URISyntaxException {

        var exporter = CsvToGraphStoreExporter.create(config(), importPath());

        var importedProperties = exporter.run(AllocationTracker.empty());
        assertThat(importedProperties).isNotNull();
        assertThat(importedProperties.nodePropertyCount()).isEqualTo(10);

        var graph = exporter.graph();
        assertThat(graph).isNotNull();
        assertThat(graph.nodeCount()).isEqualTo(10L);
        var loadedNodeSchema = graph.schema().nodeSchema();
        var expectedNodeSchema = NodeSchema.builder()
            .addProperty(
                NodeLabel.of("A"),
                "prop1",
                PropertySchema.of("prop1", ValueType.LONG, DefaultValue.of(42L), GraphStore.PropertyState.PERSISTENT)
            )
            .addProperty(NodeLabel.of("A"), "neoId", ValueType.LONG)
            .addProperty(NodeLabel.of("B"), "neoId", ValueType.LONG)
            .build();

        var testGraph = TestSupport.fromGdl(
                                            "  (:A {neoId: 1329, prop1: 21})" +
                                            ", (:A {neoId: 1330, prop1: 22})" +
                                            ", (:A {neoId: 1331, prop1: 23})" +
                                            ", (:A {neoId: 1332, prop1: 24})" +
                                            ", (:A {neoId: 1333, prop1: 25})" +
                                            ", (:B {neoId: 1328})" +
                                            ", (:B {neoId: 1334})" +
                                            ", (:B {neoId: 1335})" +
                                            ", (:B {neoId: 1336})" +
                                            ", (:B {neoId: 1337})"
        );
        assertGraphEquals(testGraph, graph);

        assertThat(loadedNodeSchema).isEqualTo(expectedNodeSchema);
    }

    private GraphStoreToFileExporterConfig config() {
        return ImmutableGraphStoreToFileExporterConfig.builder()
            .exportName("my-export")
            .writeConcurrency(1)
            .build();
    }

    private Path importPath() throws URISyntaxException {
        var uri = Objects.requireNonNull(getClass().getClassLoader().getResource("CsvToGraphStoreExporterTest")).toURI();
        return Paths.get(uri);
    }

}
