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
import org.neo4j.gds.ImmutableRelationshipProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.HugeGraphBuilder;
import org.neo4j.gds.core.loading.HighLimitIdMap;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.ImmutableImportMetaData;
import org.neo4j.gds.core.loading.ImportSizing;
import org.neo4j.gds.core.loading.RecordsBatchBuffer;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporterBuilder;
import org.neo4j.gds.core.loading.SingleTypeRelationships;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

@Value.Style(
    builderVisibility = Value.Style.BuilderVisibility.PUBLIC,
    depluralize = true,
    deepImmutablesDetection = true
)
public final class GraphFactory {

    private GraphFactory() {}

    public static NodesBuilderBuilder initNodesBuilder() {
        return new NodesBuilderBuilder();
    }

    public static NodesBuilderBuilder initNodesBuilder(NodeSchema nodeSchema) {
        return new NodesBuilderBuilder().nodeSchema(nodeSchema);
    }

    @Builder.Factory
    static NodesBuilder nodesBuilder(
        Optional<Long> maxOriginalId,
        Optional<Long> nodeCount,
        Optional<NodeSchema> nodeSchema,
        Optional<Boolean> hasLabelInformation,
        Optional<Boolean> hasProperties,
        Optional<Boolean> deduplicateIds,
        Optional<Concurrency> concurrency,
        Optional<PropertyState> propertyState,
        Optional<String> idMapBuilderType
    ) {
        boolean labelInformation = nodeSchema
            .map(schema -> !(schema.availableLabels().isEmpty() && schema.containsOnlyAllNodesLabel()))
            .or(() -> hasLabelInformation).orElse(false);
        var threadCount = concurrency.orElse(new Concurrency(1));

        var idMapBehavior = IdMapBehaviorServiceProvider.idMapBehavior();

        var idMapType = idMapBuilderType.orElse(IdMap.NO_TYPE);
        var idMapBuilder = idMapBehavior.create(
            idMapType,
            threadCount,
            maxOriginalId,
            nodeCount
        );

        long maxOriginalNodeId = maxOriginalId.orElse(NodesBuilder.UNKNOWN_MAX_ID);
        boolean deduplicate = deduplicateIds.orElse(true);
        long maxIntermediateId = maxOriginalNodeId;

        if (HighLimitIdMap.isHighLimitIdMap(idMapType)) {
            // If the requested id map is high limit, we need to make sure that
            // internal data structures are sized accordingly. Using the highest
            // original id will potentially fail due to size limitations.
            if (nodeCount.isPresent()) {
                maxIntermediateId = nodeCount.get() - 1;
            } else {
                throw new IllegalArgumentException("Cannot use high limit id map without node count.");
            }
            if (deduplicate) {
                // We internally use HABS for deduplication, which is being initialized
                // with max original id. This is fine for all id maps except high limit,
                // where original ids can exceed the supported HABS range.
                throw new IllegalArgumentException("Cannot use high limit id map with deduplication.");
            }
        }

        return nodeSchema.isPresent()
            ? fromSchema(
            maxOriginalNodeId,
                maxIntermediateId,
                idMapBuilder,
                threadCount,
                nodeSchema.get(),
                labelInformation,
                deduplicate
            )
            : new NodesBuilder(
                maxOriginalNodeId,
                maxIntermediateId,
                threadCount,
                NodesBuilderContext.lazy(threadCount),
                idMapBuilder,
                labelInformation,
                hasProperties.orElse(false),
                deduplicate,
                __ -> propertyState.orElse(PropertyState.PERSISTENT)
            );
    }

    private static NodesBuilder fromSchema(
        long maxOriginalId,
        long maxIntermediateId,
        IdMapBuilder idMapBuilder,
        Concurrency concurrency,
        NodeSchema nodeSchema,
        boolean hasLabelInformation,
        boolean deduplicateIds
    ) {
        return new NodesBuilder(
            maxOriginalId,
            maxIntermediateId,
            concurrency,
            NodesBuilderContext.fixed(nodeSchema, concurrency),
            idMapBuilder,
            hasLabelInformation,
            nodeSchema.hasProperties(),
            deduplicateIds,
            propertyKey -> nodeSchema.unionProperties().get(propertyKey).state()
        );
    }

    @ValueClass
    public interface PropertyConfig {

        String propertyKey();

        @Value.Default
        default Aggregation aggregation() {
            return Aggregation.NONE;
        }

        @Value.Default
        default DefaultValue defaultValue() {
            return DefaultValue.forDouble();
        }

        static PropertyConfig of(String propertyKey) {
            return ImmutablePropertyConfig.builder().propertyKey(propertyKey).build();
        }

        static ImmutablePropertyConfig.Builder builder() {
            return ImmutablePropertyConfig.builder();
        }

        static PropertyConfig of(String propertyKey, Aggregation aggregation, DefaultValue defaultValue) {
            return ImmutablePropertyConfig.builder()
                .propertyKey(propertyKey)
                .aggregation(aggregation)
                .defaultValue(defaultValue)
                .build();
        }

        @Value.Default
        default PropertyState propertyState() {
            return PropertyState.TRANSIENT;
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
        Optional<ExecutorService> executorService
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
        var projectionBuilder = RelationshipProjection
            .builder()
            .type(relationshipType.name())
            .orientation(actualOrientation)
            .indexInverse(indexInverse.orElse(false));

        propertyConfigs.forEach(propertyConfig -> projectionBuilder.addProperty(
            propertyConfig.propertyKey(),
            propertyConfig.propertyKey(),
            DefaultValue.of(propertyConfig.defaultValue()),
            propertyConfig.aggregation()
        ));

        var projection = projectionBuilder.build();

        int[] propertyKeyIds = IntStream.range(0, propertyConfigs.size()).toArray();
        double[] defaultValues = propertyConfigs.stream().mapToDouble(c -> c.defaultValue().doubleValue()).toArray();

        var finalConcurrency = concurrency.orElse(new Concurrency(1));
        var maybeRootNodeCount = nodes.rootNodeCount();
        var importSizing = maybeRootNodeCount.isPresent()
            ? ImportSizing.of(finalConcurrency, maybeRootNodeCount.getAsLong())
            : ImportSizing.of(finalConcurrency);

        int bufferSize = RecordsBatchBuffer.DEFAULT_BUFFER_SIZE;
        if (maybeRootNodeCount.isPresent()) {
            var rootNodeCount = maybeRootNodeCount.getAsLong();
            if (rootNodeCount > 0 && rootNodeCount < RecordsBatchBuffer.DEFAULT_BUFFER_SIZE) {
                bufferSize = (int) rootNodeCount;
            }
        }

        boolean skipDangling = skipDanglingRelationships.orElse(true);

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(projection)
            .aggregations(aggregations)
            .propertyKeyIds(propertyKeyIds)
            .defaultValues(defaultValues)
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .skipDanglingRelationships(skipDangling)
            .build();

        var singleTypeRelationshipImporter = new SingleTypeRelationshipImporterBuilder()
            .importMetaData(importMetaData)
            .nodeCountSupplier(() -> nodes.rootNodeCount().orElse(0L))
            .importSizing(importSizing)
            .build();

        var singleTypeRelationshipsBuilderBuilder = new SingleTypeRelationshipsBuilderBuilder()
            .idMap(nodes)
            .importer(singleTypeRelationshipImporter)
            .bufferSize(bufferSize)
            .relationshipType(relationshipType)
            .propertyConfigs(propertyConfigs)
            .isMultiGraph(isMultiGraph)
            .loadRelationshipProperty(loadRelationshipProperties)
            .direction(Direction.fromOrientation(actualOrientation))
            .executorService(executorService.orElse(DefaultPool.INSTANCE))
            .concurrency(finalConcurrency);

        if (indexInverse.orElse(false)) {
            var inverseProjection = ImmutableRelationshipProjection
                .builder()
                .from(projection)
                .orientation(projection.orientation().inverse())
                .build();

            var inverseImportMetaData = ImmutableImportMetaData.builder()
                .from(importMetaData)
                .projection(inverseProjection)
                .skipDanglingRelationships(skipDangling)
                .build();

            var inverseImporter = new SingleTypeRelationshipImporterBuilder()
                .importMetaData(inverseImportMetaData)
                .nodeCountSupplier(() -> nodes.rootNodeCount().orElse(0L))
                .importSizing(importSizing)
                .build();

            singleTypeRelationshipsBuilderBuilder.inverseImporter(inverseImporter);
        }

        return new RelationshipsBuilder(singleTypeRelationshipsBuilderBuilder.build(), skipDangling, finalConcurrency);
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
        var nodeSchema = MutableNodeSchema.empty();
        idMap.availableNodeLabels().forEach(nodeSchema::getOrCreateLabel);

        relationships.properties().ifPresent(relationshipPropertyStore -> {
            assert relationshipPropertyStore.values().size() == 1: "Cannot instantiate graph with more than one relationship property.";
        });

        var relationshipSchema = MutableRelationshipSchema.empty();
        relationshipSchema.set(relationships.relationshipSchemaEntry());

        return create(
            MutableGraphSchema.of(nodeSchema, relationshipSchema, Map.of()),
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
            assert relationshipPropertyStore.values().size() == 1: "Cannot instantiate graph with more than one relationship property.";
            return relationshipPropertyStore.values().iterator().next().values();
        });
        var inverseProperties = relationships.inverseProperties().map(relationshipPropertyStore -> {
            assert relationshipPropertyStore.values().size() == 1: "Cannot instantiate graph with more than one relationship property.";
            return relationshipPropertyStore.values().iterator().next().values();
        });

        var characteristicsBuilder = GraphCharacteristics.builder().withDirection(graphSchema.direction());
        relationships.inverseTopology().ifPresent(__ -> characteristicsBuilder.inverseIndexed());

        return new HugeGraphBuilder()
            .nodes(idMap)
            .schema(graphSchema)
            .characteristics(characteristicsBuilder.build())
            .nodeProperties(nodeProperties)
            .topology(topology)
            .inverseTopology(inverseTopology)
            .relationshipProperties(properties)
            .inverseRelationshipProperties(inverseProperties)
            .build();
    }
}
