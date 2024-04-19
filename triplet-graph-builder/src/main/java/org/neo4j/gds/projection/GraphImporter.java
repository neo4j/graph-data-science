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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.schema.ImmutableMutableGraphSchema;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.ImmutableNodes;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.loading.LazyIdMapBuilder;
import org.neo4j.gds.core.loading.ReadHelper;
import org.neo4j.gds.core.loading.RelationshipImportResult;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.ImmutablePropertyConfig;
import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.PropertyValues;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.ProgressTimer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;

public final class GraphImporter {
    public static final int NO_TARGET_NODE = -1;

    private final GraphProjectConfig config;
    private final List<String> undirectedRelationshipTypes;
    private final List<String> inverseIndexedRelationshipTypes;
    private final LazyIdMapBuilder idMapBuilder;

    private final WriteMode writeMode;
    private final String query;

    private final Map<RelationshipType, RelationshipsBuilder> relImporters;
    private final ImmutableMutableGraphSchema.Builder graphSchemaBuilder;

    public GraphImporter(
        GraphProjectConfig config,
        List<String> undirectedRelationshipTypes,
        List<String> inverseIndexedRelationshipTypes,
        LazyIdMapBuilder idMapBuilder,
        WriteMode writeMode,
        String query
    ) {
        this.config = config;
        this.undirectedRelationshipTypes = undirectedRelationshipTypes;
        this.inverseIndexedRelationshipTypes = inverseIndexedRelationshipTypes;
        this.idMapBuilder = idMapBuilder;
        this.writeMode = writeMode;
        this.query = query;
        this.relImporters = new ConcurrentHashMap<>();
        this.graphSchemaBuilder = MutableGraphSchema.builder();
    }

    public void update(
        long sourceNode,
        long targetNode,
        @Nullable PropertyValues sourceNodePropertyValues,
        @Nullable PropertyValues targetNodePropertyValues,
        NodeLabelToken sourceNodeLabels,
        NodeLabelToken targetNodeLabels,
        RelationshipType relationshipType,
        @Nullable PropertyValues relationshipProperties
    ) {

        var intermediateSourceId = loadNode(sourceNode, sourceNodeLabels, sourceNodePropertyValues);

        if (targetNode != NO_TARGET_NODE) {
            RelationshipsBuilder relImporter;
            // we do the check before to avoid having to create a new lambda instance on every call
            if (this.relImporters.containsKey(relationshipType)) {
                relImporter = this.relImporters.get(relationshipType);
            } else {
                relImporter = this.relImporters.computeIfAbsent(
                    relationshipType,
                    type -> newRelImporter(type, relationshipProperties)
                );
            }

            var intermediateTargetId = loadNode(targetNode, targetNodeLabels, targetNodePropertyValues);

            if (relationshipProperties != null) {
                if (relationshipProperties.size() == 1) {
                    relationshipProperties.forEach((key, value) -> {
                        var property = ReadHelper.extractValue(value, DefaultValue.DOUBLE_DEFAULT_FALLBACK);
                        relImporter.addFromInternal(intermediateSourceId, intermediateTargetId, property);
                    });
                } else {
                    var propertyValues = new double[relationshipProperties.size()];
                    int[] index = {0};
                    relationshipProperties.forEach((key, value) -> {
                        var property = ReadHelper.extractValue(value, DefaultValue.DOUBLE_DEFAULT_FALLBACK);
                        var i = index[0]++;
                        propertyValues[i] = property;
                    });
                    relImporter.addFromInternal(intermediateSourceId, intermediateTargetId, propertyValues);
                }
            } else {
                relImporter.addFromInternal(intermediateSourceId, intermediateTargetId);
            }
        }
    }

    public AggregationResult result(
        DatabaseInfo databaseInfo,
        ProgressTimer timer,
        boolean hasSeenArbitraryId
    ) {
        var graphName = config.graphName();

        if (GraphStoreCatalog.exists(config.username(), databaseInfo.databaseId(), graphName)) {
            throw new IllegalArgumentException("Graph " + graphName + " already exists");
        }

        this.idMapBuilder.prepareForFlush();

        var writeMode = hasSeenArbitraryId
            ? WriteMode.NONE
            : this.writeMode;

        var graphStoreBuilder = new GraphStoreBuilder()
            .concurrency(this.config.typedReadConcurrency())
            .capabilities(ImmutableStaticCapabilities.of(writeMode))
            .databaseInfo(databaseInfo);

        var valueMapper = buildNodesWithProperties(graphStoreBuilder);
        buildRelationshipsWithProperties(graphStoreBuilder, valueMapper);

        var graphStore = graphStoreBuilder.schema(this.graphSchemaBuilder.build()).build();

        GraphStoreCatalog.set(this.config, graphStore);

        var projectMillis = timer.stop().getDuration();

        return AggregationResultBuilder.builder()
            .graphName(graphName)
            .nodeCount(graphStore.nodeCount())
            .relationshipCount(graphStore.relationshipCount())
            .projectMillis(projectMillis)
            .configuration(this.config.asProcedureResultConfigurationField()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .query(this.query)
            .build();
    }

    private RelationshipsBuilder newRelImporter(RelationshipType relType, @Nullable PropertyValues properties) {
        var orientation = this.undirectedRelationshipTypes.contains(relType.name) || this.undirectedRelationshipTypes
            .contains(
                "*"
            )
                ? UNDIRECTED
                : NATURAL;

        boolean indexInverse = inverseIndexedRelationshipTypes.contains(relType.name) || inverseIndexedRelationshipTypes
            .contains("*");

        var relationshipsBuilderBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(this.idMapBuilder)
            .relationshipType(relType)
            .orientation(orientation)
            .aggregation(Aggregation.NONE)
            .indexInverse(indexInverse)
            .concurrency(this.config.typedReadConcurrency());

        if (properties != null) {
            for (String propertyKey : properties.propertyKeys()) {
                relationshipsBuilderBuilder.addPropertyConfig(
                    ImmutablePropertyConfig.builder().propertyKey(propertyKey).build()
                );
            }
        }

        return relationshipsBuilderBuilder.build();
    }

    /**
     * Adds the given node to the internal nodes builder and returns
     * the intermediate node id which can be used for relationships.
     *
     * @return intermediate node id
     */
    private long loadNode(
        long node,
        NodeLabelToken nodeLabels,
        @Nullable PropertyValues nodeProperties
    ) {
        return nodeProperties == null
            ? this.idMapBuilder.addNode(node, nodeLabels)
            : this.idMapBuilder.addNodeWithProperties(
                node,
                nodeProperties,
                nodeLabels
            );
    }

    private AdjacencyCompressor.ValueMapper buildNodesWithProperties(GraphStoreBuilder graphStoreBuilder) {
        var idMapAndProperties = this.idMapBuilder.build();

        var idMap = idMapAndProperties.idMap();
        var nodeSchema = idMapAndProperties.schema();

        this.graphSchemaBuilder.nodeSchema(nodeSchema);

        var nodes = ImmutableNodes
            .builder()
            .idMap(idMap)
            .schema(nodeSchema)
            .properties(idMapAndProperties.propertyStore())
            .build();

        graphStoreBuilder.nodes(nodes);

        // Relationships are added using their intermediate node ids.
        // In order to map to the final internal ids, we need to use
        // the mapping function of the wrapped id map.
        return idMap.rootIdMap()::toMappedNodeId;
    }

    private void buildRelationshipsWithProperties(
        GraphStoreBuilder graphStoreBuilder,
        AdjacencyCompressor.ValueMapper valueMapper
    ) {
        var relationshipImportResultBuilder = RelationshipImportResult.builder();

        var relationshipSchema = MutableRelationshipSchema.empty();
        this.relImporters.forEach((relationshipType, relImporter) -> {
            var relationships = relImporter.build(
                Optional.of(valueMapper),
                Optional.empty()
            );
            relationshipSchema.set(relationships.relationshipSchemaEntry());
            relationshipImportResultBuilder.putImportResult(relationshipType, relationships);
        });

        graphStoreBuilder.relationshipImportResult(relationshipImportResultBuilder.build());
        this.graphSchemaBuilder.relationshipSchema(relationshipSchema);

        // release all references to the builders
        // we are only be called once and don't support double invocations of `result` building
        this.relImporters.clear();
    }
}
