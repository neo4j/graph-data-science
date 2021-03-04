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
package org.neo4j.graphalgo.core.loading.construction;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.graphalgo.core.loading.AdjacencyBuilder;
import org.neo4j.graphalgo.core.loading.AdjacencyListWithPropertiesBuilder;
import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.loading.RecordsBatchBuffer;
import org.neo4j.graphalgo.core.loading.RelationshipImporter;
import org.neo4j.graphalgo.core.loading.SingleTypeRelationshipImporter;
import org.neo4j.graphalgo.core.loading.TransientAdjacencyListBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

@Value.Style(
    builderVisibility = Value.Style.BuilderVisibility.PUBLIC,
    depluralize = true,
    deepImmutablesDetection = true
)
public final class GraphFactory {

    private static final String DUMMY_PROPERTY = "property";

    private GraphFactory() {}

    public static NodesBuilderBuilder initNodesBuilder() {
        return new NodesBuilderBuilder();
    }

    @Builder.Factory
    static NodesBuilder nodesBuilder(
        long maxOriginalId,
        Optional<Boolean> hasLabelInformation,
        Optional<Integer> concurrency,
        Optional<AllocationTracker> tracker
    ) {
        return new NodesBuilder(
            maxOriginalId,
            hasLabelInformation.orElse(false),
            concurrency.orElse(1),
            tracker.orElse(AllocationTracker.empty())
        );
    }

    public static RelationshipsBuilderBuilder initRelationshipsBuilder() {
        return new RelationshipsBuilderBuilder();
    }

    @Builder.Factory
    static RelationshipsBuilder relationshipsBuilder(
        IdMapping nodes,
        Optional<Orientation> orientation,
        Optional<Boolean> loadRelationshipProperty,
        Optional<Aggregation> aggregation,
        Optional<DefaultValue> defaultValue,
        Optional<Boolean> preAggregate,
        Optional<Integer> concurrency,
        Optional<ExecutorService> executorService,
        // TODO: non-optional
        Optional<AllocationTracker> tracker
    ) {
        List<PropertyConfig> propertyConfigs = loadRelationshipProperty.orElse(false)
            ? List.of(PropertyConfig.of(aggregation, defaultValue))
            : List.of();

        return relationshipsWithMultiplePropertiesBuilder(
            nodes,
            orientation,
            propertyConfigs,
            preAggregate,
            concurrency,
            executorService,
            tracker
        );
    }

    @ValueClass
    public interface PropertyConfig {

        @Value.Default
        default Aggregation aggregation() {
            return Aggregation.NONE;
        }

        @Value.Default
        default DefaultValue defaultValue() {
            return DefaultValue.forDouble();
        }

        static PropertyConfig of(Aggregation aggregation, DefaultValue defaultValue) {
            return ImmutablePropertyConfig.of(aggregation, defaultValue);
        }

        static PropertyConfig of(Optional<Aggregation> aggregation, Optional<DefaultValue> defaultValue) {
            return of(aggregation.orElse(Aggregation.NONE), defaultValue.orElse(DefaultValue.forDouble()));
        }
    }

    public static RelationshipsWithMultiplePropertiesBuilderBuilder initRelationshipsWithMultiplePropertiesBuilder() {
        return new RelationshipsWithMultiplePropertiesBuilderBuilder();
    }

    @Builder.Factory
    static RelationshipsBuilder relationshipsWithMultiplePropertiesBuilder(
        IdMapping nodes,
        Optional<Orientation> orientation,
        List<PropertyConfig> propertyConfigs,
        Optional<Boolean> preAggregate,
        Optional<Integer> concurrency,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> tracker
    ) {
        var loadRelationshipProperties = !propertyConfigs.isEmpty();
        var actualTracker = tracker.orElse(AllocationTracker.empty());

        var aggregations = propertyConfigs.isEmpty()
            ? new Aggregation[]{Aggregation.NONE}
            : propertyConfigs.stream()
                .map(GraphFactory.PropertyConfig::aggregation)
                .map(Aggregation::resolve)
                .toArray(Aggregation[]::new);

        var relationshipType = RelationshipType.ALL_RELATIONSHIPS;
        var isMultiGraph = Arrays.stream(aggregations).allMatch(Aggregation::equivalentToNone);

        var projectionBuilder = RelationshipProjection
            .builder()
            .type(relationshipType.name())
            .orientation(orientation.orElse(Orientation.NATURAL));

        propertyConfigs.forEach(propertyConfig -> projectionBuilder.addProperty(
            GraphFactory.DUMMY_PROPERTY,
            GraphFactory.DUMMY_PROPERTY,
            DefaultValue.of(propertyConfig.defaultValue()),
            propertyConfig.aggregation()
        ));

        var projection = projectionBuilder.build();

        int[] propertyKeyIds = IntStream.range(0, propertyConfigs.size()).toArray();
        double[] defaultValues = propertyConfigs.stream().mapToDouble(c -> c.defaultValue().doubleValue()).toArray();

        var importSizing = ImportSizing.of(concurrency.orElse(1), nodes.rootNodeCount());
        int pageSize = importSizing.pageSize();
        int numberOfPages = importSizing.numberOfPages();
        int bufferSize = (int) Math.min(nodes.rootNodeCount(), RecordsBatchBuffer.DEFAULT_BUFFER_SIZE);

        var relationshipCounter = new LongAdder();

        var adjacencyListWithPropertiesBuilder = AdjacencyListWithPropertiesBuilder.create(
            nodes.rootNodeCount(),
            projection,
            TransientAdjacencyListBuilder.builderFactory(actualTracker),
            TransientAdjacencyOffsets.forPageSize(pageSize),
            aggregations,
            propertyKeyIds,
            defaultValues,
            actualTracker
        );

        AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
            adjacencyListWithPropertiesBuilder,
            numberOfPages,
            pageSize,
            actualTracker,
            relationshipCounter,
            preAggregate.orElse(false)
        );

        var relationshipImporter = new RelationshipImporter(actualTracker, adjacencyBuilder);

        var importerBuilder = new SingleTypeRelationshipImporter.Builder(
            relationshipType,
            projection,
            loadRelationshipProperties,
            NO_SUCH_RELATIONSHIP_TYPE,
            relationshipImporter,
            relationshipCounter,
            false
        ).loadImporter(loadRelationshipProperties);

        return new RelationshipsBuilder(
            nodes,
            orientation.orElse(Orientation.NATURAL),
            bufferSize,
            propertyKeyIds,
            adjacencyListWithPropertiesBuilder,
            importerBuilder,
            relationshipCounter,
            loadRelationshipProperties,
            isMultiGraph,
            concurrency.orElse(1),
            executorService.orElse(Pools.DEFAULT)
        );
    }

    public static HugeGraph create(NodeMapping idMap, Relationships relationships, AllocationTracker tracker) {
        var nodeSchemaBuilder = NodeSchema.builder();
        idMap.availableNodeLabels().forEach(nodeSchemaBuilder::addLabel);
        return create(
            idMap,
            nodeSchemaBuilder.build(),
            Collections.emptyMap(),
            relationships,
            tracker
        );
    }

    public static HugeGraph create(
        NodeMapping idMap,
        NodeSchema nodeSchema,
        Map<String, NodeProperties> nodeProperties,
        Relationships relationships,
        AllocationTracker tracker
    ) {
        var relationshipSchemaBuilder = RelationshipSchema.builder();
        if (relationships.properties().isPresent()) {
            relationshipSchemaBuilder.addProperty(
                RelationshipType.of("REL"),
                "property",
                ValueType.DOUBLE
            );
        } else {
            relationshipSchemaBuilder.addRelationshipType(RelationshipType.of("REL"));
        }
        return HugeGraph.create(
            idMap,
            GraphSchema.of(nodeSchema, relationshipSchemaBuilder.build()),
            nodeProperties,
            relationships.topology(),
            relationships.properties(),
            tracker
        );
    }
}
