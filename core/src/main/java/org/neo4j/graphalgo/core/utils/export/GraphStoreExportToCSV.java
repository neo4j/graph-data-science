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

import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.utils.export.file.FileExporter;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;

public class GraphStoreExportToCSV {

    private final GraphStore graphStore;

    private final GraphStoreFileExportConfig config;

    public GraphStoreExportToCSV(
        GraphStore graphStore,
        GraphStoreFileExportConfig config
    ) {
        this.graphStore = graphStore;
        this.config = config;
    }

    public GraphStoreExport.ImportedProperties run(AllocationTracker tracker) {
        var nodeStore = NodeStore.of(graphStore, tracker);
        var relationshipStore = RelationshipStore.of(graphStore, config.defaultRelationshipType());
        var graphStoreInput = new GraphStoreInput(
            nodeStore,
            relationshipStore,
            DEFAULT_BATCH_SIZE
        );

        FileExporter.csv(graphStoreInput, graphStore.schema(), config.exportLocationPath(), config.writeConcurrency()).export();

        long importedNodeProperties = nodeStore.propertyCount() * graphStore.nodes().nodeCount();
        long importedRelationshipProperties = relationshipStore.propertyCount() * graphStore.relationshipCount();
        return ImmutableImportedProperties.of(importedNodeProperties, importedRelationshipProperties);
    }
}
