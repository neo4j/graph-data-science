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
package org.neo4j.graphalgo.core.utils.export;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyList;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.eclipse.collections.impl.tuple.Tuples.pair;

public class RelationshipStore {

    final long nodeCount;
    final long relationshipCount;

    final Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators;

    RelationshipStore(
        long nodeCount,
        long relationshipCount,
        Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators
    ) {
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        this.relationshipIterators = relationshipIterators;
    }

    public long propertyCount() {
        return relationshipIterators.values().stream().mapToInt(CompositeRelationshipIterator::propertyCount).sum();
    }

    RelationshipStore concurrentCopy() {
        return new RelationshipStore(
            nodeCount,
            relationshipCount,
            relationshipIterators.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().concurrentCopy()
            ))
        );
    }

    static RelationshipStore of(GraphStore graphStore, String defaultRelationshipType) {
        Map<RelationshipType, Relationships.Topology> topologies = new HashMap<>();
        Map<RelationshipType, Map<String, Relationships.Properties>> properties = new HashMap<>();

        graphStore.relationshipTypes().stream()
            // extract (relationshipType, propertyKey) tuples
            .flatMap(relType -> graphStore.relationshipPropertyKeys(relType).isEmpty()
                ? Stream.of(pair(relType, Optional.<String>empty()))
                : graphStore
                    .relationshipPropertyKeys(relType)
                    .stream()
                    .map(propertyKey -> pair(relType, Optional.of(propertyKey))))
            // extract graph for relationship type and property
            .map(relTypeAndProperty -> pair(
                relTypeAndProperty,
                graphStore.getGraph(relTypeAndProperty.getOne(), relTypeAndProperty.getTwo())
            ))
            // extract Topology list and associated Properties lists
            .forEach(relTypeAndPropertyAndGraph -> {
                var relationshipType = relTypeAndPropertyAndGraph.getOne().getOne();
                var maybePropertyKey = relTypeAndPropertyAndGraph.getOne().getTwo();
                var graph = relTypeAndPropertyAndGraph.getTwo();

                topologies.computeIfAbsent(relationshipType, ignored -> ((HugeGraph) graph).relationshipTopology());
                maybePropertyKey.ifPresent(propertyKey -> properties
                    .computeIfAbsent(relationshipType, ignored -> new HashMap<>())
                    // .get() is safe, since we have a property key
                    .put(propertyKey, ((HugeGraph) graph).relationships().properties().get()));
            });

        Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators = new HashMap<>();

        // for each relationship type, merge its Topology list and all associated Property lists
        topologies.forEach((relationshipType, topology) -> {
            var adjacencyDegrees = topology.degrees();
            var adjacencyList = topology.list();
            var adjacencyOffsets = topology.offsets();

            var propertyLists = properties.getOrDefault(relationshipType, Map.of())
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> (TransientAdjacencyList) entry.getValue().list()
                ));

            var propertyOffsets = properties.getOrDefault(relationshipType, Map.of())
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> (TransientAdjacencyOffsets) entry.getValue().offsets()
                ));

            // iff relationshipType is '*', change it the given default
            var outputRelationshipType = relationshipType.equals(RelationshipType.ALL_RELATIONSHIPS)
                ? RelationshipType.of(defaultRelationshipType)
                : relationshipType;

            relationshipIterators.put(
                outputRelationshipType,
                new CompositeRelationshipIterator(
                    adjacencyDegrees,
                    adjacencyList,
                    adjacencyOffsets,
                    propertyLists,
                    propertyOffsets
                )
            );
        });

        return new RelationshipStore(
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            relationshipIterators
        );
    }
}
