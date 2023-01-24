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

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public interface ElementSchemaEntry<SELF extends ElementSchemaEntry<SELF, ELEMENT_IDENTIFIER, PROPERTY_SCHEMA>, ELEMENT_IDENTIFIER extends ElementIdentifier, PROPERTY_SCHEMA extends PropertySchema> {

    ELEMENT_IDENTIFIER identifier();

    Map<String, PROPERTY_SCHEMA> properties();

    SELF union(SELF other);

    Map<String, Object> toMap();

    default Map<String, PROPERTY_SCHEMA> unionProperties(Map<String, PROPERTY_SCHEMA> rightProperties) {
        return Stream
            .concat(properties().entrySet().stream(), rightProperties.entrySet().stream())
            .collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (leftType, rightType) -> {
                    if (leftType.valueType() != rightType.valueType()) {
                        throw new IllegalArgumentException(format(Locale.ENGLISH,
                            "Combining schema entries with value type %s and %s is not supported.",
                            properties()
                                .entrySet()
                                .stream()
                                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().valueType())),
                            rightProperties
                                .entrySet()
                                .stream()
                                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().valueType()))
                        ));
                    } else {
                        return leftType;
                    }
                }));
    }
}
