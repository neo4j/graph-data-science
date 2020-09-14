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
package org.neo4j.graphalgo.api.schema;

import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface ElementSchema<SELF extends ElementSchema<SELF, I>, I extends ElementIdentifier> {

    Map<I, Map<String, ValueType>> properties();

    SELF filter(Set<I> elementIdentifieresToKeep);

    SELF union(SELF other);

    default Map<String, Object> toMap() {
        return properties().entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().name,
            entry -> entry
                .getValue()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    innerEntry -> GraphSchema.forValueType(innerEntry.getValue()))
                )
        ));
    }

    default Map<I, Map<String, ValueType>> filterProperties(Set<I> identifiersToKeep) {
        return properties()
            .entrySet()
            .stream()
            .filter(entry -> identifiersToKeep.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    }
    default Map<I, Map<String, ValueType>> unionProperties(
        Map<I, Map<String, ValueType>> rightProperties
    ) {
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
                    if (leftType != rightType) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Combining schema entries with value type %s and %s is not supported.",
                            left,
                            right
                        ));
                    } else {
                        return leftType;
                    }
                }
            ))
        ));
    }
}
