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
package org.neo4j.gds.api.schema;

import org.neo4j.gds.RelationshipType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface RelationshipSchema {
    Set<RelationshipType> availableTypes();

    boolean isUndirected();

    boolean isUndirected(RelationshipType type);

    // TODO: remove
    Map<RelationshipType, Direction> directions();

    RelationshipSchema filter(Set<RelationshipType> relationshipTypesToKeep);

    RelationshipSchema union(RelationshipSchema other);

    Collection<MutableRelationshipSchemaEntry> entries();

    MutableRelationshipSchemaEntry get(RelationshipType identifier);

    default Set<String> allProperties() {
        return entries()
            .stream()
            .flatMap(entry -> entry.properties().keySet().stream())
            .collect(Collectors.toSet());
    }

    default Set<String> allProperties(RelationshipType relationshipType) {
        return Optional.ofNullable(get(relationshipType))
            .map(entry -> entry.properties().keySet())
            .orElse(Set.of());
    }

    default boolean hasProperties() {
        return entries().stream().anyMatch(entry -> !entry.properties().isEmpty());
    }

    default boolean hasProperty(RelationshipType relationshipType, String propertyKey) {
        return Optional.ofNullable(get(relationshipType))
            .map(entry -> entry.properties().containsKey(propertyKey))
            .orElse(false);
    }

    default List<RelationshipPropertySchema> propertySchemasFor(RelationshipType relationshipType) {
        return Optional
            .ofNullable(get(relationshipType))
            .map(entry -> entry.properties().values())
            .map(ArrayList::new)
            .orElse(new ArrayList<>());
    }

    default Map<String, RelationshipPropertySchema> unionProperties() {
        return entries()
            .stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (leftSchema, rightSchema) -> leftSchema));
    }

    default Map<RelationshipType, MutableRelationshipSchemaEntry> unionEntries(RelationshipSchema other) {
        return Stream
            .concat(entries().stream(), other.entries().stream())
            .collect(Collectors.toMap(MutableRelationshipSchemaEntry::identifier, Function.identity(), MutableRelationshipSchemaEntry::union));
    }
}
