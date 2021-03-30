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

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilderBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class GraphStoreRelationshipVisitor extends RelationshipVisitor {

    private final Map<String, RelationshipsBuilder> relationshipBuilders;
    private final RelationshipsBuilderBuilder relationshipsBuilderBuilder;

    protected GraphStoreRelationshipVisitor(RelationshipSchema relationshipSchema, RelationshipsBuilderBuilder relationshipsBuilderBuilder) {
        super(relationshipSchema);
        this.relationshipsBuilderBuilder = relationshipsBuilderBuilder;
        this.relationshipBuilders = new HashMap<>();

    }

    @Override
    protected void exportElement() {
        var relationshipsBuilder = relationshipBuilders.computeIfAbsent(
            relationshipType(),
            (key) -> relationshipsBuilderBuilder.build()
        );

        relationshipsBuilder.add(startNode(), endNode());
    }

    protected RelationshipVisitorResult result() {
        var resultBuilder = ImmutableRelationshipVisitorResult.builder();
        var relationshipCountTracker = new LongAdder();
        var relationshipTypeTopologyMap = relationshipBuilders.entrySet().stream().map(entry -> {
            var type = entry.getKey();
            var builder = entry.getValue();
            var topology = builder.build().topology();
            relationshipCountTracker.add(topology.elementCount());
            return Map.entry(RelationshipType.of(type), topology);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return resultBuilder
            .putAllRelationshipTypesWithTopology(relationshipTypeTopologyMap)
            .relationshipCount(relationshipCountTracker.longValue())
            .build();
    }

    @ValueClass
    interface RelationshipVisitorResult {
        Map<RelationshipType, Relationships.Topology> relationshipTypesWithTopology();
        long relationshipCount();
    }
}
