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
package org.neo4j.graphalgo.core.utils.export;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.utils.export.file.FileExporter;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.batchimport.Configuration;

import java.nio.file.Path;

import static org.neo4j.io.ByteUnit.mebiBytes;

public class GraphStoreExportToCSV {

    private final GraphStore graphStore;

    private final GraphStoreExportConfig config;
    private final Path exportPath;

    public GraphStoreExportToCSV(
        GraphStore graphStore,
        GraphStoreExportConfig config,
        Path exportPath
    ) {
        this.graphStore = graphStore;
        this.config = config;
        this.exportPath = exportPath;
    }

    public GraphStoreExport.ImportedProperties run(AllocationTracker tracker) {
        var nodeStore = GraphStoreExport.NodeStore.of(graphStore, tracker);
        var relationshipStore = GraphStoreExport.RelationshipStore.of(graphStore, config.defaultRelationshipType());
        var graphStoreInput = new GraphStoreInput(
            nodeStore,
            relationshipStore,
            config.batchSize()
        );

        FileExporter.csv(graphStoreInput, graphStore, exportPath).doExport();

        long importedNodeProperties = nodeStore.propertyCount() * graphStore.nodes().nodeCount();
        long importedRelationshipProperties = relationshipStore.propertyCount() * graphStore.relationshipCount();
        return ImmutableImportedProperties.of(importedNodeProperties, importedRelationshipProperties);
    }

    @NotNull
    private Configuration getImportConfig(boolean defaultSettingsSuitableForTests) {
        return new Configuration() {
            @Override
            public int maxNumberOfProcessors() {
                return config.writeConcurrency();
            }

            @Override
            public long pageCacheMemory() {
                return defaultSettingsSuitableForTests ? mebiBytes(8) : Configuration.super.pageCacheMemory();
            }

            @Override
            public boolean highIO() {
                return false;
            }
        };
    }
}
