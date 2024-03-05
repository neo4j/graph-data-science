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

import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.schema.ElementSchema;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.MutableGraphSchema;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.PropertySchema;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

final class SchemaHelper {

    private SchemaHelper() {}

    static GraphSchema filterInvisibleProperties(GraphSchema schema) {
        return MutableGraphSchema.of(
            (MutableNodeSchema) filterInvisibleProperties(schema.nodeSchema()),
            (MutableRelationshipSchema) filterInvisibleProperties(schema.relationshipSchema()),
            filterInvisibleProperties(schema.graphProperties())
        );
    }

    private static <S extends ElementSchema<S, ?, ?, ?>> S filterInvisibleProperties(S schema) {
        return schema.filterProperties(not(propertySchema -> propertySchema.state() == PropertyState.AUXILIARY));
    }

    private static Map<String, PropertySchema> filterInvisibleProperties(Map<String, PropertySchema> propertySchemas) {
        return propertySchemas.entrySet().stream()
            .filter(not(entry -> entry.getValue().state() == PropertyState.AUXILIARY))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
