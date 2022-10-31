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

import org.immutables.value.Value;
import org.neo4j.gds.ElementIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ElementSchema<SELF extends ElementSchema<SELF, ELEMENT_IDENTIFIER, PROPERTY_SCHEMA>, ELEMENT_IDENTIFIER extends ElementIdentifier, PROPERTY_SCHEMA extends PropertySchema> {

    Map<ELEMENT_IDENTIFIER, Map<String, PROPERTY_SCHEMA>> properties();

    SELF filter(Set<ELEMENT_IDENTIFIER> elementIdentifieresToKeep);

    SELF union(SELF other);

    @Value.Derived
    default Set<String> allProperties() {
        return properties()
            .values()
            .stream()
            .flatMap(propertyMapping -> propertyMapping.keySet().stream())
            .collect(Collectors.toSet());
    }

    @Value.Derived
    default boolean hasProperties() {
        return !allProperties().isEmpty();
    }

    @Value.Default
    default boolean hasProperties(ELEMENT_IDENTIFIER elementIdentifier) {
        return !properties().get(elementIdentifier).isEmpty();
    }

    @Value.Default
    default boolean hasProperty(ELEMENT_IDENTIFIER elementIdentifier, String propertyKey) {
        return properties().containsKey(elementIdentifier) && (!properties()
            .get(elementIdentifier)
            .isEmpty() && properties().get(elementIdentifier).containsKey(propertyKey));
    }

    @Value.Default
    default List<PROPERTY_SCHEMA> propertySchemasFor(ELEMENT_IDENTIFIER elementIdentifier) {
        var propertySchemaForTypes = filter(Set.of(elementIdentifier));
        return new ArrayList<>(propertySchemaForTypes.unionProperties().values());
    }

    @Value.Derived
    default Map<ELEMENT_IDENTIFIER, Map<String, PROPERTY_SCHEMA>> filterProperties(Set<ELEMENT_IDENTIFIER> identifiersToKeep) {
        return properties()
            .entrySet()
            .stream()
            .filter(entry -> identifiersToKeep.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new HashMap<>(entry.getValue())));

    }

    /**
     * Returns a union of all properties in the given schema.
     */
    @Value.Lazy
    default Map<String, PROPERTY_SCHEMA> unionProperties() {
        return properties()
            .values()
            .stream()
            .flatMap(e -> e.entrySet().stream())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (leftSchema, rightSchema) -> leftSchema
            ));
    }

    /**
     * For internal use only!
     */
    default Map<ELEMENT_IDENTIFIER, Map<String, PROPERTY_SCHEMA>> unionSchema(Map<ELEMENT_IDENTIFIER, Map<String, PROPERTY_SCHEMA>> rightProperties) {
        return Stream.concat(
            properties().entrySet().stream(),
            rightProperties.entrySet().stream()
        ).collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (left, right) -> Stream.concat(
                left.entrySet().stream(),
                right.entrySet().stream()
            ).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (leftType, rightType) -> {
                    if (leftType.valueType() != rightType.valueType()) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "Combining schema entries with value type %s and %s is not supported.",
                            left.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().valueType())),
                            right.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().valueType()))
                        ));
                    } else {
                        return leftType;
                    }
                }
            ))
        ));
    }
}
