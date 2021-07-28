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
package org.neo4j.graphalgo.core.utils.io;

import org.neo4j.common.Validator;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;

public abstract class GraphStoreExporter<CONFIG extends GraphStoreExporterBaseConfig> {

    private final GraphStore graphStore;

    protected final CONFIG config;

    private final Map<String, LongFunction<Object>> neoNodeProperties;

    protected GraphStoreExporter(GraphStore graphStore, CONFIG config) {
        this(graphStore, config, Optional.empty());
    }

    protected GraphStoreExporter(
        GraphStore graphStore,
        CONFIG config,
        Optional<NeoNodeProperties> neoNodeProperties
    ) {
        this.graphStore = graphStore;
        this.config = config;
        this.neoNodeProperties = neoNodeProperties
            .map(NeoNodeProperties::neoNodeProperties)
            .orElse(Map.of());
    }

    protected abstract void export(GraphStoreInput graphStoreInput);

    public ImportedProperties run(AllocationTracker tracker) {
        var metaDataStore = MetaDataStore.of(graphStore);
        var nodeStore = NodeStore.of(graphStore, neoNodeProperties, tracker);
        var relationshipStore = RelationshipStore.of(graphStore, config.defaultRelationshipType());
        var graphStoreInput = new GraphStoreInput(
            metaDataStore,
            nodeStore,
            relationshipStore,
            config.batchSize()
        );

        export(graphStoreInput);

        long importedNodeProperties = (nodeStore.propertyCount() + neoNodeProperties.size()) * graphStore.nodeCount();
        long importedRelationshipProperties = relationshipStore.propertyCount() * graphStore.relationshipCount();
        return ImmutableImportedProperties.of(importedNodeProperties, importedRelationshipProperties);
    }

    @ValueClass
    public interface ImportedProperties {

        long nodePropertyCount();

        long relationshipPropertyCount();
    }

    public static final Validator<Path> DIRECTORY_IS_WRITABLE = value -> {
        try {
            // TODO: A validator should only validate, not create the directory as well
            Files.createDirectories(value);
            if (!Files.isDirectory(value)) {
                throw new IllegalArgumentException("'" + value + "' is not a directory");
            }
            if (!Files.isWritable(value)) {
                throw new IllegalArgumentException("Directory '" + value + "' not writable");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Directory '" + value + "' not writable: ", e);
        }
    };
}
