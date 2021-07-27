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

import org.jetbrains.annotations.Nullable;
import org.neo4j.common.Validator;
import org.neo4j.graphalgo.ImmutablePropertyMapping;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.TransactionContext;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public abstract class GraphStoreExporter<CONFIG extends GraphStoreExporterBaseConfig> {

    private final GraphStore graphStore;
    protected final CONFIG config;
    private final TransactionContext transactionContext;

    protected GraphStoreExporter(GraphStore graphStore, CONFIG config) {
        this(null, graphStore, config);
    }

    protected GraphStoreExporter(TransactionContext transactionContext, GraphStore graphStore, CONFIG config) {
        this.transactionContext = transactionContext;
        this.graphStore = graphStore;
        this.config = config;
    }

    protected abstract void export(GraphStoreInput graphStoreInput);

    public ImportedProperties run(AllocationTracker tracker) {

        var prop4Mapping = ImmutablePropertyMapping.builder()
            .propertyKey("prop4")
            .defaultValue(DefaultValue.DEFAULT)
            .neoPropertyKey("prop4")
            .build();

        var prop5Mapping = ImmutablePropertyMapping.builder()
            .propertyKey("prop5")
            .defaultValue(DefaultValue.DEFAULT)
            .neoPropertyKey("prop5")
            .build();

        Map<String, Map<String, NodeProperties>> additionalProperties = new HashMap<>(Map.of(
            "A", new HashMap<>(Map.of("prop4", new StringProperties(transactionContext, graphStore.nodes(), prop4Mapping))),
            "B", new HashMap<>(Map.of("prop5", new StringProperties(transactionContext, graphStore.nodes(), prop5Mapping)))
        ));

        var metaDataStore = MetaDataStore.of(graphStore);
        var nodeStore = NodeStore.of(graphStore, tracker, additionalProperties);
        var relationshipStore = RelationshipStore.of(graphStore, config.defaultRelationshipType());
        var graphStoreInput = new GraphStoreInput(
            metaDataStore,
            nodeStore,
            relationshipStore,
            config.batchSize()
        );

        export(graphStoreInput);

        long importedNodeProperties = nodeStore.propertyCount() * graphStore.nodes().nodeCount();
        long importedRelationshipProperties = relationshipStore.propertyCount() * graphStore.relationshipCount();
        return ImmutableImportedProperties.of(importedNodeProperties, importedRelationshipProperties);
    }

    static class StringProperties implements NodeProperties {

        final TransactionContext transactionContext;
        final NodeMapping nodeMapping;
        private final PropertyMapping propertyMapping;


        StringProperties(TransactionContext transactionContext, NodeMapping nodeMapping, PropertyMapping propertyMapping) {
            this.transactionContext = transactionContext;
            this.nodeMapping = nodeMapping;
            this.propertyMapping = propertyMapping;
        }

        @Override
        public @Nullable Object getObject(long nodeId) {
            long originalId = nodeMapping.toOriginalNodeId(nodeId);

            return transactionContext.apply((tx, ktx) -> tx
                .getNodeById(originalId)
                .getProperty(propertyMapping.neoPropertyKey(), propertyMapping.defaultValue().getObject()));
        }

        @Override
        public ValueType valueType() {
            return ValueType.UNKNOWN;
        }

        @Override
        public Value value(long nodeId) {
            return Values.stringValue((String) getObject(nodeId));
        }

        @Override
        public long size() {
            // TODO
            return 0;
        }
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
