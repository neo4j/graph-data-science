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
package org.neo4j.graphalgo.core.loading.construction;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class GraphFactory {

    static final String DUMMY_PROPERTY = "property";

    private GraphFactory() {}

    public static NodesBuilder nodesBuilder(
        long maxOriginalId,
        boolean hasLabelInformation,
        int concurrency,
        AllocationTracker tracker
    ) {
        return new NodesBuilder(
            maxOriginalId,
            hasLabelInformation,
            concurrency,
            tracker
        );
    }

    public static RelationshipsBuilder relationshipsBuilder(
        IdMap idMap,
        Orientation orientation,
        boolean loadRelationshipProperty,
        Aggregation aggregation,
        boolean preAggregate,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return relationshipsBuilder(
            idMap,
            orientation,
            loadRelationshipProperty,
            aggregation,
            preAggregate,
            1,
            executorService,
            tracker
        );
    }

    public static RelationshipsBuilder relationshipsBuilder(
        IdMap idMap,
        Orientation orientation,
        boolean loadRelationshipProperty,
        Aggregation aggregation,
        boolean preAggregate,
        int concurrency,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        return new RelationshipsBuilder(
            idMap,
            orientation,
            loadRelationshipProperty,
            aggregation,
            preAggregate,
            concurrency,
            executorService,
            tracker
        );
    }

    public static HugeGraph create(IdMap idMap, Relationships relationships, AllocationTracker tracker) {
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
        IdMap idMap,
        Map<String, NodeProperties> nodeProperties,
        Relationships relationships,
        AllocationTracker tracker
    ) {
        if (nodeProperties.isEmpty()) {
            return create(idMap, relationships, tracker);
        } else {
            var nodeSchemaBuilder = NodeSchema.builder();
            nodeProperties.forEach((propertyName, property) -> nodeSchemaBuilder.addProperty(
                NodeLabel.ALL_NODES,
                propertyName,
                property.valueType()
            ));
            return create(
                idMap,
                nodeSchemaBuilder.build(),
                nodeProperties,
                relationships,
                tracker
            );
        }
    }

    public static HugeGraph create(
        IdMap idMap,
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
