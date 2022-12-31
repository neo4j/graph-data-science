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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.ObjectIntScatterMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.HugeGraphBuilder;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.ImmutableImportMetaData;
import org.neo4j.gds.core.loading.ImportSizing;
import org.neo4j.gds.core.loading.RecordsBatchBuffer;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImporterBuilder;
import org.neo4j.gds.core.loading.nodeproperties.NodePropertiesFromStoreBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.neo4j.gds.core.GraphDimensions.ANY_LABEL;
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
        long maxOriginalId,
        Optional<Long> nodeCount,
        Optional<NodeSchema> nodeSchema,
        Optional<Boolean> hasLabelInformation,
        Optional<Boolean> hasProperties,
        Optional<Boolean> deduplicateIds,
        Optional<Integer> concurrency
    ) {
        boolean labelInformation = nodeSchema
            .map(schema -> !(schema.availableLabels().isEmpty() && schema.containsOnlyAllNodesLabel()))
            .or(() -> hasLabelInformation).orElse(false);
        int threadCount = concurrency.orElse(1);

        var idMapBehavior = IdMapBehaviorServiceProvider.idMapBehavior();
        var maybeMaxOriginalId = maxOriginalId != NodesBuilder.UNKNOWN_MAX_ID
            ? Optional.of(maxOriginalId)
            : Optional.<Long>empty();

        var idMapBuilder = idMapBehavior.create(
            threadCount,
            maybeMaxOriginalId,
            nodeCount
        );

        boolean deduplicate = deduplicateIds.orElse(true);

        return nodeSchema.map(schema -> fromSchema(
            maxOriginalId,
            idMapBuilder,
            threadCount,
            schema,
            labelInformation,
            deduplicate
        )).orElseGet(() -> new NodesBuilder(
            maxOriginalId,
            threadCount,
            new ObjectIntScatterMap<>(),
            new IntObjectHashMap<>(),
            new ConcurrentHashMap<>(),
            idMapBuilder,
            labelInformation,
            hasProperties.orElse(false),
            deduplicate
        ));
    }

    private static NodesBuilder fromSchema(
        long maxOriginalId,
        IdMapBuilder idMapBuilder,
        int concurrency,
        NodeSchema nodeSchema,
        boolean hasLabelInformation,
        boolean deduplicateIds
    ) {
        var nodeLabels = nodeSchema.availableLabels();

        var elementIdentifierLabelTokenMapping = new ObjectIntScatterMap<NodeLabel>();
        var labelTokenNodeLabelMapping = new IntObjectHashMap<List<NodeLabel>>();
        var labelTokenCounter = new MutableInt(0);
        nodeLabels.forEach(nodeLabel -> {
            int labelToken = nodeLabel == NodeLabel.ALL_NODES
                ? ANY_LABEL
                : labelTokenCounter.getAndIncrement();

            elementIdentifierLabelTokenMapping.put(nodeLabel, labelToken);
            labelTokenNodeLabelMapping.put(labelToken, List.of(nodeLabel));
        });

        var propertyBuildersByPropertyKey = nodeSchema.unionProperties().entrySet().stream().collect(toMap(
            Map.Entry::getKey,
            e -> NodePropertiesFromStoreBuilder.of(e.getValue().defaultValue(), concurrency)
        ));

        return new NodesBuilder(
            maxOriginalId,
            concurrency,
            elementIdentifierLabelTokenMapping,
            labelTokenNodeLabelMapping,
            new ConcurrentHashMap<>(propertyBuildersByPropertyKey),
            idMapBuilder,
            hasLabelInformation,
            nodeSchema.hasProperties(),
            deduplicateIds
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

        static PropertyConfig of(String propertyKey, Aggregation aggregation, DefaultValue defaultValue) {
            return ImmutablePropertyConfig.of(propertyKey, aggregation, defaultValue);
        }

        static PropertyConfig of(String propertyKey, Optional<Aggregation> aggregation, Optional<DefaultValue> defaultValue) {
            return of(propertyKey, aggregation.orElse(Aggregation.NONE), defaultValue.orElse(DefaultValue.forDouble()));
        }
    }

    public static RelationshipsBuilderBuilder initRelationshipsBuilder() {
        return new RelationshipsBuilderBuilder();
    }

    @Builder.Factory
    static RelationshipsBuilder relationshipsBuilder(
        PartialIdMap nodes,
        Optional<Orientation> orientation,
        List<PropertyConfig> propertyConfigs,
        Optional<Aggregation> aggregation,
        Optional<Boolean> validateRelationships,
        Optional<Integer> concurrency,
        Optional<Boolean> indexInverse,
        Optional<ExecutorService> executorService
    ) {
        var loadRelationshipProperties = !propertyConfigs.isEmpty();

        var aggregations = propertyConfigs.isEmpty()
            ? new Aggregation[]{aggregation.orElse(Aggregation.NONE)}
            : propertyConfigs.stream()
                .map(GraphFactory.PropertyConfig::aggregation)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

        var relationshipType = RelationshipType.ALL_RELATIONSHIPS;
        var isMultiGraph = Arrays.stream(aggregations).allMatch(Aggregation::equivalentToNone);

        var actualOrientation = orientation.orElse(Orientation.NATURAL);
        var projectionBuilder = RelationshipProjection
            .builder()
            .type(relationshipType.name())
            .orientation(actualOrientation);

        propertyConfigs.forEach(propertyConfig -> projectionBuilder.addProperty(
            propertyConfig.propertyKey(),
            propertyConfig.propertyKey(),
            DefaultValue.of(propertyConfig.defaultValue()),
            propertyConfig.aggregation()
        ));

        var projection = projectionBuilder.build();

        int[] propertyKeyIds = IntStream.range(0, propertyConfigs.size()).toArray();
        String[] propertyKeys = new String[propertyConfigs.size()];
        for (int propertyKeyId : propertyKeyIds) {
            propertyKeys[propertyKeyId] = propertyConfigs.get(propertyKeyId).propertyKey();
        }

        double[] defaultValues = propertyConfigs.stream().mapToDouble(c -> c.defaultValue().doubleValue()).toArray();

        int finalConcurrency = concurrency.orElse(1);
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

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(projection)
            .aggregations(aggregations)
            .propertyKeyIds(propertyKeyIds)
            .defaultValues(defaultValues)
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .build();

        var singleTypeRelationshipImporter = new SingleTypeRelationshipImporterBuilder()
            .importMetaData(importMetaData)
            .nodeCountSupplier(() -> nodes.rootNodeCount().orElse(0L))
            .importSizing(importSizing)
            .validateRelationships(validateRelationships.orElse(false))
            .build();

        var singleTypeRelationshipsBuilder = new SingleTypeRelationshipsBuilderBuilder()
            .idMap(nodes)
            .importer(singleTypeRelationshipImporter)
            .bufferSize(bufferSize)
            .propertyKeyIds(propertyKeyIds)
            .propertyKeys(propertyKeys)
            .aggregations(aggregations)
            .isMultiGraph(isMultiGraph)
            .loadRelationshipProperty(loadRelationshipProperties)
            .direction(Direction.fromOrientation(actualOrientation))
            .executorService(executorService.orElse(Pools.DEFAULT))
            .concurrency(finalConcurrency)
            .build();

        return new RelationshipsBuilder(singleTypeRelationshipsBuilder);
    }

    /**
     * Creates a {@link org.neo4j.gds.core.huge.HugeGraph} from the given node and relationship data.
     *
     * The node schema will be inferred from the available node labels.
     * The relationship schema will use default relationship type {@code "REL"}.
     * If a relationship property is present, the default relationship property key {@code "property"}
     * will be used.
     */
    public static HugeGraph create(IdMap idMap, SingleTypeRelationshipImportResult relationships) {
        var nodeSchema = NodeSchema.empty();
        idMap.availableNodeLabels().forEach(nodeSchema::getOrCreateLabel);

        relationships.properties().ifPresent(relationshipPropertyStore -> {
            assert relationshipPropertyStore.values().size() == 1: "Cannot instantiate graph with more than one relationship property.";
        });

        var relationshipSchema = relationships.relationshipSchema(RelationshipType.of("REL"));

        return create(
            GraphSchema.of(nodeSchema, relationshipSchema, Map.of()),
            idMap,
            Map.of(),
            relationships
        );
    }

    public static HugeGraph create(
        GraphSchema graphSchema,
        IdMap idMap,
        Map<String, NodePropertyValues> nodeProperties,
        SingleTypeRelationshipImportResult relationships
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

        return new HugeGraphBuilder()
            .nodes(idMap)
            .schema(graphSchema)
            .nodeProperties(nodeProperties)
            .topology(topology)
            .inverseTopology(inverseTopology)
            .relationshipProperties(properties)
            .inverseRelationshipProperties(inverseProperties)
            .build();
    }
}
