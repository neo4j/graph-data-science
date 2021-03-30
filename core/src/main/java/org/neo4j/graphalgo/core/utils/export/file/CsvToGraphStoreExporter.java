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

import org.immutables.builder.Builder;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.ImmutableNodePropertyStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperty;
import org.neo4j.graphalgo.api.NodePropertyStore;
import org.neo4j.graphalgo.api.RelationshipPropertyStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.utils.export.GraphStoreExporter;
import org.neo4j.graphalgo.core.utils.export.ImmutableImportedProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public final class CsvToGraphStoreExporter {

    private final GraphStoreNodeVisitor.Builder nodeVisitorBuilder;
    private final Path importPath;
    private final GraphStoreToFileExporterConfig config;

    private final GraphStoreBuilder graphStoreBuilder;

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
        this.graphStoreBuilder = new GraphStoreBuilder().concurrency(config.writeConcurrency());
    }

    public GraphStore graphStore() {
        return graphStoreBuilder.build();
    }

    public GraphStoreExporter.ImportedProperties run(AllocationTracker tracker) {
        var fileInput = new FileInput(importPath);
        graphStoreBuilder.tracker(tracker);
        return export(fileInput, tracker);
    }

    private GraphStoreExporter.ImportedProperties export(FileInput fileInput, AllocationTracker tracker) {
        var nodes = exportNodes(fileInput, tracker);
        var exportedRelationships = exportRelationships(fileInput, nodes, AllocationTracker.empty());
        return ImmutableImportedProperties.of(nodes.nodeCount(), exportedRelationships);
    }

    private NodeMapping exportNodes(
        FileInput fileInput,
        AllocationTracker tracker
    ) {
        NodeSchema nodeSchema = fileInput.nodeSchema();

        GraphInfo graphInfo = fileInput.graphInfo();
        graphStoreBuilder.databaseId(graphInfo.namedDatabaseId());

        int concurrency = config.writeConcurrency();
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder(nodeSchema)
            .maxOriginalId(graphInfo.maxOriginalId())
            .concurrency(concurrency)
            .nodeCount(graphInfo.nodeCount())
            .tracker(tracker)
            .build();
        nodeVisitorBuilder.withNodeSchema(nodeSchema);
        nodeVisitorBuilder.withNodesBuilder(nodesBuilder);

        var nodesIterator = fileInput.nodes(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner(nodeVisitorBuilder.build(), nodesIterator)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var nodeMappingAndProperties = nodesBuilder.build();
        graphStoreBuilder.nodes(nodeMappingAndProperties.nodeMapping());
        nodeMappingAndProperties.nodeProperties().orElse(Map.of())
            .forEach((label, propertyMap) -> {
                var nodeStoreProperties = propertyMap.entrySet().stream().map(entry -> {
                    var propertyKey = entry.getKey();
                    var nodeProperties = entry.getValue();
                    var propertySchema = nodeSchema.properties().get(label).get(propertyKey);
                    var nodeProperty = NodeProperty.of(
                        propertySchema.key(),
                        propertySchema.state(),
                        nodeProperties,
                        propertySchema.defaultValue()
                    );

                    return Map.entry(propertyKey, nodeProperty);
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                graphStoreBuilder.putNodePropertyStores(label, ImmutableNodePropertyStore.of(nodeStoreProperties));
            });

        return nodeMappingAndProperties.nodeMapping();
    }

    private long exportRelationships(FileInput fileInput, NodeMapping nodes, AllocationTracker tracker) {
        var relationshipSchema = fileInput.relationshipSchema();
        int concurrency = config.writeConcurrency();
        var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
            .concurrency(concurrency)
            .nodes(nodes)
            .tracker(tracker);

        var relationshipVisitor = new GraphStoreRelationshipVisitor(relationshipSchema, relationshipsBuilderBuilder);
        var relationshipsIterator = fileInput.relationships(Collector.EMPTY).iterator();
        Collection<Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> new ElementImportRunner(relationshipVisitor, relationshipsIterator)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var relationshipVisitorResult = relationshipVisitor.result();
        graphStoreBuilder.relationships(relationshipVisitorResult.relationshipTypesWithTopology());

        return relationshipVisitorResult.relationshipCount();
    }

    @Builder.Factory
    static GraphStore graphStore(
        NamedDatabaseId databaseId,
        NodeMapping nodes,
        Map<NodeLabel, NodePropertyStore> nodePropertyStores,
        Map<RelationshipType, Relationships.Topology> relationships,
        Map<RelationshipType, RelationshipPropertyStore> relationshipPropertyStores,
        int concurrency,
        AllocationTracker tracker
    ) {
        return CSRGraphStore.of(
            databaseId,
            nodes,
            nodePropertyStores,
            relationships,
            relationshipPropertyStores,
            concurrency,
            tracker
        );
    }
}
