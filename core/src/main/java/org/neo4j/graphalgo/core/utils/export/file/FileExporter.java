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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.utils.export.Exporter;
import org.neo4j.graphalgo.core.utils.export.GraphStoreInput;
import org.neo4j.graphalgo.core.utils.export.file.csv.CsvNodeVisitor;
import org.neo4j.internal.batchimport.input.Collector;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

public final class FileExporter extends Exporter {
    private final Supplier<NodeVisitor> nodeVisitorSupplier;

    public static FileExporter csv(
        GraphStoreInput graphStoreInput,
        GraphSchema graphSchema,
        Path exportLocation
    ) {
        return new FileExporter(
            graphStoreInput,
            () -> new CsvNodeVisitor(exportLocation, graphSchema)
        );
    }

    private FileExporter(
        GraphStoreInput graphStoreInput,
        Supplier<NodeVisitor> nodeVisitorSupplier
    ) {
        super(graphStoreInput);
        this.nodeVisitorSupplier = nodeVisitorSupplier;
    }

    @Override
    public void export() {
        exportNodes();
        exportRelationships();
    }

    private void exportNodes() {
        var nodeInput = graphStoreInput.nodes(Collector.EMPTY);
        var nodeInputIterator = nodeInput.iterator();
        var nodeVisitor = nodeVisitorSupplier.get();

        try (var chunk = nodeInputIterator.newChunk()) {
            while (nodeInputIterator.next(chunk)) {
                while (chunk.next(nodeVisitor)) {

                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        nodeVisitor.close();
    }

    void exportRelationships() {}

}
