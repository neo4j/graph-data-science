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

import org.neo4j.gds.ElementIdentifier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ElementSchema<
    SELF extends ElementSchema<SELF, ELEMENT_IDENTIFIER, ENTRY, PROPERTY_SCHEMA>,
    ELEMENT_IDENTIFIER extends ElementIdentifier,
    ENTRY extends ElementSchemaEntry<ENTRY, ELEMENT_IDENTIFIER, PROPERTY_SCHEMA>,
    PROPERTY_SCHEMA extends PropertySchema> {


    SELF filter(Set<ELEMENT_IDENTIFIER> elementIdentifiersToKeep);

    SELF union(SELF other);

    Collection<ENTRY> entries();

    ENTRY get(ELEMENT_IDENTIFIER identifier);

    default Set<String> allProperties() {
        return entries()
            .stream()
            .flatMap(entry -> entry.properties().keySet().stream())
            .collect(Collectors.toSet());
    }

    default Set<String> allProperties(ELEMENT_IDENTIFIER elementIdentifier) {
        return entries()
            .stream()
            .flatMap(entry -> entry.properties().keySet().stream())
            .collect(Collectors.toSet());
    }

    default boolean hasProperties() {
        return entries().stream().anyMatch(entry -> !entry.properties().isEmpty());
    }

    default boolean hasProperty(ELEMENT_IDENTIFIER elementIdentifier, String propertyKey) {
        return Optional.ofNullable(get(elementIdentifier))
            .map(entry -> entry.properties().containsKey(propertyKey))
            .orElse(false);
    }

    default List<PROPERTY_SCHEMA> propertySchemasFor(ELEMENT_IDENTIFIER elementIdentifier) {
        return Optional
            .ofNullable(get(elementIdentifier))
            .map(entry -> entry.properties().values())
            .map(ArrayList::new)
            .orElse(new ArrayList<>());
    }

    default Map<String, PROPERTY_SCHEMA> unionProperties() {
        return entries()
            .stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (leftSchema, rightSchema) -> leftSchema));
    }

    default Map<String, Object> toMap() {
        return entries()
            .stream()
            .collect(Collectors.toMap(entry -> entry.identifier().name, ElementSchemaEntry::toMap));
    }

    default Map<ELEMENT_IDENTIFIER, ENTRY> unionEntries(SELF other) {
        return Stream
            .concat(entries().stream(), other.entries().stream())
            .collect(Collectors.toMap(ElementSchemaEntry::identifier, Function.identity(), ElementSchemaEntry::union));
    }
}
