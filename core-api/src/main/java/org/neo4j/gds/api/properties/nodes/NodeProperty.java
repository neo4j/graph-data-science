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
package org.neo4j.gds.api.properties.nodes;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.Property;
import org.neo4j.gds.api.schema.PropertySchema;

import java.util.OptionalInt;

@ValueClass
@SuppressWarnings("immutables:from")
public interface NodeProperty extends Property<NodePropertyValues> {

    static NodeProperty of(
        String key,
        PropertyState origin,
        NodePropertyValues values
    ) {
        return of(key, origin, values, values.valueType().fallbackValue());
    }

    static NodeProperty of(
        String key,
        PropertyState origin,
        NodePropertyValues values,
        DefaultValue defaultValue
    ) {
        return ImmutableNodeProperty.of(
            values,
            PropertySchema.of(key, values.valueType(), defaultValue, origin, vectorDimension(values))
        );
    }

    /**
     * Only a vector property carries a dimension in its schema; for every other type the dimension is a
     * property of the values, not of the schema.
     */
    private static OptionalInt vectorDimension(NodePropertyValues values) {
        if (!values.valueType().isVector()) {
            return OptionalInt.empty();
        }
        return values.dimension().map(OptionalInt::of).orElseGet(OptionalInt::empty);
    }
}
