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

import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.utils.export.GraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.ImmutableImportedProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.batchimport.input.Collector;

import java.io.IOException;
import java.nio.file.Path;

public final class CsvToGraphStoreExporter {

    private final GraphStoreNodeVisitor.Builder nodeVisitorBuilder;
    private final Path importPath;
    private final GraphStoreToFileExporterConfig config;

    public static CsvToGraphStoreExporter create(
        GraphStoreToFileExporterConfig config,
        Path importPath
    ) {
        return new CsvToGraphStoreExporter(
            new GraphStoreNodeVisitor.Builder(),
            config,
            importPath
        );
    }

    private CsvToGraphStoreExporter(
        GraphStoreNodeVisitor.Builder nodeVisitorBuilder,
        GraphStoreToFileExporterConfig config,
        Path importPath
    ) {
        this.config = config;
        this.nodeVisitorBuilder = nodeVisitorBuilder.withReverseIdMapping(config.includeMetaData());
        this.importPath = importPath;
    }

    public GraphStoreExporter.ImportedProperties run(AllocationTracker tracker) {
        var fileInput = new FileInput(importPath);
        export(fileInput);
        return ImmutableImportedProperties.of(0, 0);
    }

    private void export(FileInput fileInput) {
        exportNodes(fileInput);
        exportRelationships(fileInput);
    }

    private void exportNodes(FileInput fileInput) {
        NodeSchema nodeSchema = fileInput.nodeSchema();
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder()
            .hasLabelInformation(!nodeSchema.availableLabels().isEmpty())
            .maxOriginalId(1337L) // TODO
            .concurrency(config.writeConcurrency())
            .build();
        nodeVisitorBuilder.withNodeSchema(nodeSchema);
        nodeVisitorBuilder.withNodesBuilder(nodesBuilder);
        GraphStoreNodeVisitor graphStoreNodeVisitor = nodeVisitorBuilder.build();

        var nodesIterator = fileInput.nodes(Collector.EMPTY).iterator();
        try (var chunk = nodesIterator.newChunk()) {
            while (nodesIterator.next(chunk)) {
                while (chunk.next(graphStoreNodeVisitor)) {

                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void exportRelationships(FileInput fileInput) {}

}
