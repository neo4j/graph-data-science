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

import org.neo4j.graphalgo.core.utils.export.GraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.ImmutableImportedProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.nio.file.Path;

public final class FileToGraphStoreExporter {

    private final VisitorProducer<NodeVisitor> nodeVisitorVisitorSupplier;
    private final Path importPath;
    private GraphStoreToFileExporterConfig config;

    private FileToGraphStoreExporter(
        VisitorProducer<NodeVisitor> nodeVisitorVisitorSupplier,
        GraphStoreToFileExporterConfig config,
        Path importPath
    ) {
        this.config = config;
        this.nodeVisitorVisitorSupplier = nodeVisitorVisitorSupplier;
        this.importPath = importPath;
    }

    public GraphStoreExporter.ImportedProperties run(AllocationTracker tracker) {
        var fileInput = new FileInput();
        export(fileInput);
        return ImmutableImportedProperties.of(0, 0);
    }

    private void export(FileInput fileInput) {
        exportMetaData(fileInput);
        exportNodes(fileInput);
        exportRelationships(fileInput);
    }

    private void exportMetaData(FileInput fileInput) {}

    private void exportNodes(FileInput fileInput) {}

    private void exportRelationships(FileInput fileInput) {}

}
