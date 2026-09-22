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
package org.neo4j.gds.core.loading.construction;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.gds.Aggregation;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMapping.Key;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.AdjacencyListBehavior;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.ImportSizing;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.RecordsBatchBuffer;
import org.neo4j.gds.core.loading.ShardedIdMapBuilder;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.gds.core.loading.SingleTypeRelationships;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

@Value.Style(
    builderVisibility = Value.Style.BuilderVisibility.PUBLIC,
    depluralize = true,
    deepImmutablesDetection = true
)
public final class GraphFactory {
    private static final int NO_SUCH_RELATIONSHIP_TYPE = -1;

    private GraphFactory() {}

    public static NodesBuilderBuilder initNodesBuilder() {
        return new NodesBuilderBuilder();
    }

    private static IdMapBuilder initIdMapBuilder(String idMapBuilderType, Concurrency concurrency, Optional<Long> maxOriginalId, Optional<Long> nodeCount, boolean deduplicateIds) {
        if (ShardedIdMapBuilder.isShardedIdMapType(idMapBuilderType) && deduplicateIds) {
            // We internally use HABS for deduplication, which is being initialized
            // with max original id. This is fine for all id maps except high limit,
            // where original ids can exceed the supported HABS range.
            throw new IllegalArgumentException("Cannot use high limit id map with deduplication.");
        }
        return IdMapBehaviorServiceProvider.idMapBehavior()
            .create(idMapBuilderType, concurrency, maxOriginalId, nodeCount);
    }

    private static long computeMaxIntermediateId(long maxOriginalNodeId, Optional<Long> nodeCount, String idMapBuilderType) {
        long maxIntermediateId = maxOriginalNodeId;
        if (ShardedIdMapBuilder.isShardedIdMapType(idMapBuilderType)) {
            // If the requested id map is high limit, we need to make sure that
            // internal data structures are sized accordingly. Using the highest
            // original id will potentially fail due to size limitations.
            // If the node count is not given, we fall back to an unknown max id,
            // which is fine since label building relies on growing bitsets.
            maxIntermediateId = nodeCount.map(nc -> nc - 1).orElse(NodesBuilder.UNKNOWN_MAX_ID);
        }
        return maxIntermediateId;
    }

    public record PropertyInfo(boolean hasProperties, Function<String, PropertyState> propertyStateFunction) {
        public static final PropertyInfo DEFAULT = new PropertyInfo(false, __ -> PropertyState.PERSISTENT);

        public static PropertyInfo of(boolean hasProperties, PropertyState state) {
            return new PropertyInfo(hasProperties, __ -> state);
        }
        public static PropertyInfo withState(PropertyState state) {
            return new PropertyInfo(true, __ -> state);
        }
        public static PropertyInfo withDefaultState() {
            return new PropertyInfo(true, __ -> PropertyState.PERSISTENT);
        }
        private static PropertyInfo fromSchema(NodeSchema nodeSchema) {
            return new PropertyInfo(
                nodeSchema.hasProperties(),
                propertyKey -> nodeSchema.properties().get(propertyKey).state()
            );
        }
    }

    static NodesBuilder nodesBuilder(
        Optional<Long> maxOriginalId,
        Optional<Long> nodeCount,
        NodeSchema nodeSchema,
        boolean hasLabelInformation,
        boolean deduplicateIds,
        Concurrency concurrency,
        String idMapBuilderType,
        boolean usePooledBuilderProvider,
        Optional<PropertyInfo> propertyInfo
    ) {
        if (nodeSchema.containsOtherThanAllNodesLabel()) hasLabelInformation = true;

        PropertyInfo effectivePropertyInfo;
        if (nodeSchema.isEmpty()) {
            effectivePropertyInfo = propertyInfo.orElse(PropertyInfo.DEFAULT);
        } else {
            if (propertyInfo.isPresent()) {
                throw new IllegalArgumentException(
                    "Cannot set propertyInfo when nodeSchema is provided; property info is derived from the schema."
                );
            }
            effectivePropertyInfo = PropertyInfo.fromSchema(nodeSchema);
        }

        // Make sure that we do not pass on negative values to the id map builders.
        maxOriginalId = maxOriginalId.filter(id -> id > 0);
        var idMapBuilder = initIdMapBuilder(idMapBuilderType, concurrency, maxOriginalId, nodeCount, deduplicateIds);

        long maxOriginalNodeId = maxOriginalId.orElse(NodesBuilder.UNKNOWN_MAX_ID);
        long maxIntermediateId = computeMaxIntermediateId(maxOriginalNodeId, nodeCount, idMapBuilderType);

        var labelInformationBuilder = !hasLabelInformation
            ? LabelInformationBuilders.allNodes()
            : LabelInformationBuilders.multiLabelWithCapacity(maxIntermediateId + 1);

        var nodesBuilderContext = nodeSchema.isEmpty()
            ? NodesBuilderContext.lazy(concurrency)
            : NodesBuilderContext.fixed(nodeSchema, concurrency);

        return new NodesBuilder(
            maxOriginalNodeId,
            labelInformationBuilder,
            concurrency,
            idMapBuilder,
            hasLabelInformation,
            deduplicateIds,
            usePooledBuilderProvider,
            nodesBuilderContext,
            effectivePropertyInfo
        );
    }

    public record PropertyConfig(
        String propertyKey,
        Aggregation aggregation, // default NONE
        DefaultValue defaultValue, // default DefaultValue.forDouble
        PropertyState propertyState
    ) {
        public PropertyConfig(String propertyKey, Aggregation aggregation, DefaultValue defaultValue) {
            this(propertyKey, aggregation, defaultValue, PropertyState.TRANSIENT);
        }
        public PropertyConfig(String propertyKey, Aggregation aggregation) {
            this(propertyKey, aggregation, DefaultValue.forDouble(), PropertyState.TRANSIENT);
        }
        public PropertyConfig(String propertyKey) {
            this(propertyKey, Aggregation.NONE);
        }
    }

    public static RelationshipsBuilderBuilder initRelationshipsBuilder() {
        return new RelationshipsBuilderBuilder();
    }

    @Builder.Factory
    static RelationshipsBuilder relationshipsBuilder(
        PartialIdMap nodes,
        RelationshipType relationshipType,
        Optional<Orientation> orientation,
        List<PropertyConfig> propertyConfigs,
        Optional<Aggregation> aggregation,
        Optional<Boolean> skipDanglingRelationships,
        Optional<Concurrency> concurrency,
        Optional<Boolean> indexInverse,
        Optional<ExecutorService> executorService,
        Optional<Boolean> usePooledBuilderProvider,
        Optional<AdjacencyListBehavior.Factory> adjacencyCompressorFactory
    ) {
        var loadRelationshipProperties = !propertyConfigs.isEmpty();

        var aggregations = propertyConfigs.isEmpty()
            ? new Aggregation[]{aggregation.orElse(Aggregation.DEFAULT)}
            : propertyConfigs.stream()
                .map(GraphFactory.PropertyConfig::aggregation)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

        var isMultiGraph = Arrays.stream(aggregations).allMatch(Aggregation::equivalentToNone);

        var actualOrientation = orientation.orElse(Orientation.NATURAL);
        var propertyMappings = propertyConfigs.stream()
            .map(propertyConfig -> PropertyMapping.of(
                Key.simple(propertyConfig.propertyKey()),
                DefaultValue.of(propertyConfig.defaultValue()),
                propertyConfig.aggregation()
            )).toList();
        var projection = new RelationshipProjection(
            relationshipType.name(),
            actualOrientation,
            indexInverse.orElse(false),
            PropertyMappings.of(propertyMappings)
        );

        int[] propertyKeyIds = IntStream.range(0, propertyConfigs.size()).toArray();
        double[] defaultValues = propertyConfigs.stream().mapToDouble(c -> c.defaultValue().doubleValue()).toArray();

        var finalConcurrency = concurrency.orElse(new Concurrency(1));
        long nodeCount = nodes.rootNodeCount();
        var importSizing = nodeCount == 0 ? ImportSizing.of(finalConcurrency) : ImportSizing.of(finalConcurrency, nodeCount);

        int bufferSize = RecordsBatchBuffer.DEFAULT_BUFFER_SIZE;
        if (nodeCount > 0 && nodeCount < RecordsBatchBuffer.DEFAULT_BUFFER_SIZE) {
            bufferSize = (int) nodeCount;
        }

        boolean skipDangling = skipDanglingRelationships.orElse(true);

        var importMetaData = new SingleTypeRelationshipImporter.ImportMetaData(
            projection,
            aggregations,
            propertyKeyIds,
            defaultValues,
            NO_SUCH_RELATIONSHIP_TYPE,
            skipDangling
        );

        LongSupplier nodeCountSupplier = nodes::rootNodeCount;
        var singleTypeRelationshipImporter = adjacencyCompressorFactory.map(factory ->
            SingleTypeRelationshipImporter.of(importMetaData, nodeCountSupplier, importSizing, factory))
            .orElseGet(() -> SingleTypeRelationshipImporter.of(importMetaData, nodeCountSupplier, importSizing));

        SingleTypeRelationshipsBuilder singleTypeRelationshipsBuilder;
        if (indexInverse.orElse(false)) {
            var inverseProjection = projection.inverse();

            var inverseImportMetaData = new SingleTypeRelationshipImporter.ImportMetaData(
                inverseProjection,
                importMetaData.aggregations(),
                importMetaData.propertyKeyIds(),
                importMetaData.defaultValues(),
                importMetaData.typeTokenId(),
                skipDangling
            );


            var inverseImporter = SingleTypeRelationshipImporter.of(
                inverseImportMetaData,
                nodeCountSupplier,
                importSizing
            );

            singleTypeRelationshipsBuilder = new SingleTypeRelationshipsBuilder.Indexed(
                nodes,
                singleTypeRelationshipImporter,
                inverseImporter,
                bufferSize,
                relationshipType,
                propertyConfigs,
                isMultiGraph,
                loadRelationshipProperties,
                Direction.fromOrientation(actualOrientation),
                executorService.orElse(DefaultPool.INSTANCE),
                finalConcurrency
            );
        } else {
            singleTypeRelationshipsBuilder = new SingleTypeRelationshipsBuilder.NonIndexed(
                nodes,
                singleTypeRelationshipImporter,
                bufferSize,
                relationshipType,
                propertyConfigs,
                isMultiGraph,
                loadRelationshipProperties,
                Direction.fromOrientation(actualOrientation),
                executorService.orElse(DefaultPool.INSTANCE),
                finalConcurrency
            );
        }

        var localBuilderProvider = usePooledBuilderProvider.orElse(false)
            ? LocalRelationshipsBuilderProvider
                .pooled(singleTypeRelationshipsBuilder::threadLocalRelationshipsBuilder, finalConcurrency)
            : LocalRelationshipsBuilderProvider
                .threadLocal(singleTypeRelationshipsBuilder::threadLocalRelationshipsBuilder);

        return new RelationshipsBuilder(singleTypeRelationshipsBuilder, localBuilderProvider, skipDangling);
    }

    /**
     * Creates a {@link org.neo4j.gds.core.huge.HugeGraph} from the given node and relationship data.
     *
     * The node schema will be inferred from the available node labels.
     * The relationship schema will use default relationship type {@code "REL"}.
     * If a relationship property is present, the default relationship property key {@code "property"}
     * will be used.
     */
    public static HugeGraph create(IdMap idMap, SingleTypeRelationships relationships) {
        var nodeSchema = idMap.availableNodeLabels().stream()
            .map(NodeLabel::name)
            .collect(
                NodeSchema::builder,
                NodeSchema.NodeSchemaBuilder::addLabel,
                NodeSchema.NodeSchemaBuilder::addBuilder
            ).build();

        relationships.properties().ifPresent(relationshipPropertyStore -> {
            assert relationshipPropertyStore.values()
                .size() == 1 : "Cannot instantiate graph with more than one relationship property.";
        });

        var relationshipSchema = MutableRelationshipSchema.empty();
        relationshipSchema.set(relationships.relationshipSchemaEntry());

        return create(
            MutableGraphSchema.of(nodeSchema, relationshipSchema),
            idMap,
            Map.of(),
            relationships
        );
    }

    public static HugeGraph create(
        GraphSchema graphSchema,
        IdMap idMap,
        Map<String, NodePropertyValues> nodeProperties,
        SingleTypeRelationships relationships
    ) {
        var topology = relationships.topology();
        var inverseTopology = relationships.inverseTopology();

        var properties = relationships.properties().map(relationshipPropertyStore -> {
            assert relationshipPropertyStore.values()
                .size() == 1 : "Cannot instantiate graph with more than one relationship property.";
            return relationshipPropertyStore.values().iterator().next().values();
        });
        var inverseProperties = relationships.inverseProperties().map(relationshipPropertyStore -> {
            assert relationshipPropertyStore.values()
                .size() == 1 : "Cannot instantiate graph with more than one relationship property.";
            return relationshipPropertyStore.values().iterator().next().values();
        });

        var characteristicsBuilder = GraphCharacteristics.builder().withDirection(graphSchema.direction());
        relationships.inverseTopology().ifPresent(__ -> characteristicsBuilder.inverseIndexed());

        return HugeGraph.create(
            idMap,
            graphSchema,
            characteristicsBuilder.build(),
            nodeProperties,
            topology,
            properties,
            inverseTopology,
            inverseProperties
        );
    }
}
