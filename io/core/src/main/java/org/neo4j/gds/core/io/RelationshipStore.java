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
package org.neo4j.gds.core.io;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

public final class RelationshipStore {

    final long nodeCount;
    final long relationshipCount;
    private final long propertyCount;

    final Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators;
    private final IdMap idMap;

    private RelationshipStore(
        IdMap idMap,
        long relationshipCount,
        long propertyCount,
        Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators
    ) {
        this.idMap = idMap;
        this.nodeCount = idMap.nodeCount();
        this.relationshipCount = relationshipCount;
        this.propertyCount = propertyCount;
        this.relationshipIterators = relationshipIterators;
    }

    long propertyCount() {
        return propertyCount;
    }

    public IdMap idMap() {
        return idMap;
    }

    RelationshipStore concurrentCopy() {
        var copyIterators = relationshipIterators.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().concurrentCopy()
        ));

        return new RelationshipStore(
            idMap,
            relationshipCount,
            propertyCount,
            copyIterators
        );
    }

    static RelationshipStore of(GraphStore graphStore, String defaultRelationshipType) {
        Map<RelationshipType, CompositeRelationshipIterator> relationshipIterators = new HashMap<>();
        var propertyCount = new MutableLong(0);

        graphStore.schema()
            .relationshipSchema()
            .filterProperties(not(property -> property.state() == PropertyState.HIDDEN))
            .entries()
            .forEach(entry -> {
                var relationshipType = entry.identifier();
                var properties = entry.properties().keySet();
                propertyCount.add(properties.size() * graphStore.relationshipCount(relationshipType));

                relationshipType = relationshipType.equals(RelationshipType.ALL_RELATIONSHIPS)
                    ? RelationshipType.of(defaultRelationshipType)
                    : relationshipType;

                relationshipIterators.put(
                    relationshipType,
                    graphStore.getCompositeRelationshipIterator(
                        relationshipType,
                        properties
                    )
                );
            });

        return new RelationshipStore(
            graphStore.nodes(),
            graphStore.relationshipCount(),
            propertyCount.getValue(),
            relationshipIterators
        );
    }
}
