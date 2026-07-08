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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class NodeSchemaUtils {
    private NodeSchemaUtils() {}

    public static Map<String, Object> toMap(NodeSchemaRecord nodeSchema) {
        var result = new HashMap<String, Object>();

        nodeSchema.entries().forEach((nodeLabel, propertySchemas) -> {
            if (propertySchemas.isEmpty()) {
                result.put(nodeLabel.name(), Map.of());

            } else {
                var properties = propertySchemas.stream()
                    .collect(Collectors.toMap(
                            PropertySchema::key,
                            NodeSchemaUtils::toStringRepresentation
                        )
                    );
                result.put(nodeLabel.name(), properties);
            }
        });

        return result;
    }

    private static String toStringRepresentation(PropertySchema propertySchema) {
        return String.format(
            Locale.ENGLISH,
            "%s (%s, %s)",
            propertySchema.valueType().cypherName(),
            propertySchema.defaultValue(),
            propertySchema.state()
        );
    }
}
