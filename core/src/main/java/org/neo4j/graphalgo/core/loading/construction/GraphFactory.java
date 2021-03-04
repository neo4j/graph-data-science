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
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
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
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

@Value.Style(
    builderVisibility = Value.Style.BuilderVisibility.PUBLIC,
    depluralize = true,
    deepImmutablesDetection = true
)
public final class GraphFactory {

    static final String DUMMY_PROPERTY = "property";

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
        Optional<Boolean> preAggregate,
        Optional<Integer> concurrency,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> tracker
    ) {
        return new RelationshipsBuilder(
            nodes,
            orientation.orElse(Orientation.NATURAL),
            loadRelationshipProperty.orElse(false),
            aggregation.orElse(Aggregation.NONE),
            preAggregate.orElse(false),
            concurrency.orElse(1),
            executorService.orElse(Pools.DEFAULT),
            tracker.orElse(AllocationTracker.empty())
        );
    }

    @ValueClass
    public interface PropertyConfig {

        @Value.Default
        default Aggregation aggregation() {
            return Aggregation.NONE;
        }

        @Value.Default
        default boolean preAggregate() {
            return false;
        }

        static PropertyConfig of(Aggregation aggregation, boolean preAggregate) {
            return ImmutablePropertyConfig.of(aggregation, preAggregate);
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
        Optional<Integer> concurrency,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> tracker
    ) {
        return null;
//        return new RelationshipsBuilder(
//            nodes,
//            orientation.orElse(Orientation.NATURAL),
//            loadRelationshipProperty.orElse(false),
//            aggregation.orElse(Aggregation.NONE),
//            preAggregate.orElse(false),
//            concurrency.orElse(1),
//            executorService.orElse(Pools.DEFAULT),
//            tracker.orElse(AllocationTracker.empty())
//        );
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
