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

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class RelationshipSchemaUtils {
    private RelationshipSchemaUtils() {}

    public static Map<String, Object> toMap(RelationshipSchema schema) {
        return schema.entries()
            .stream()
            .collect(Collectors.toMap(entry -> entry.identifier().name, RelationshipSchemaUtils::toMap));
    }

    public static Map<String, Object> toMapOld(RelationshipSchema schema) {
        return schema.entries()
            .stream()
            .collect(Collectors.toMap(e -> e.identifier().name(),
                e -> e
                    .properties()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        innerEntry -> toStringRepresentation(innerEntry.getValue())
                    ))
            ));
    }

    public static Map<String, Object> toMap(RelationshipSchemaEntry entry) {
        return Map.of(
            "direction", entry.direction().name(),
            "properties", entry.properties()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        innerEntry -> toStringRepresentation(innerEntry.getValue())
                    )
                )
        );
    }

    public static String toStringRepresentation(RelationshipPropertySchema propertySchema) {
        return String.format(
            Locale.ENGLISH,
            "%s (%s, %s, Aggregation.%s)",
            propertySchema.valueType().cypherName(),
            propertySchema.defaultValue(),
            propertySchema.state(),
            propertySchema.aggregation());
    }
}
