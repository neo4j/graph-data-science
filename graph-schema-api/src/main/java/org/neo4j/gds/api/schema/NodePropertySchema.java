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

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.OptionalInt;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public record NodePropertySchema(
    String key,
    ValueType valueType,
    DefaultValue defaultValue,
    PropertyState state,
    OptionalInt dimension
) implements PropertySchema {
    public NodePropertySchema {
        if (valueType.isVector() && dimension.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Property `%s` of type `%s` must have a dimension, but has none.",
                key,
                valueType.csvName()
            ));
        }
        if (!valueType.isVector() && dimension.isPresent()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Property `%s` of type `%s` cannot have a dimension, but got %d.",
                key,
                valueType.csvName(),
                dimension.getAsInt()
            ));
        }
        if (dimension.isPresent() && dimension.getAsInt() <= 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Property `%s` must have a positive dimension, but got %d.",
                key,
                dimension.getAsInt()
            ));
        }
        if (dimension.isPresent()) {
            var defaultVector = defaultValue.doubleArrayValue();
            if (defaultVector != null && defaultVector.length != dimension.getAsInt()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Property `%s` has dimension %d, but its default value has %d elements.",
                    key,
                    dimension.getAsInt(),
                    defaultVector.length
                ));
            }
        }
    }

    public static NodePropertySchema of(String propertyKey, ValueType valueType) {
        return of(propertyKey, valueType, valueType.fallbackValue(), PropertyState.PERSISTENT);
    }

    public static NodePropertySchema of(String propertyKey, ValueType valueType, DefaultValue defaultValue) {
        return of(propertyKey, valueType, defaultValue, PropertyState.PERSISTENT);
    }

    public static NodePropertySchema of(String propertyKey, ValueType valueType, DefaultValue defaultValue, PropertyState propertyState) {
        return of(propertyKey, valueType, defaultValue, propertyState, OptionalInt.empty());
    }

    public static NodePropertySchema of(
        String propertyKey,
        ValueType valueType,
        DefaultValue defaultValue,
        PropertyState propertyState,
        OptionalInt dimension
    ) {
        return new NodePropertySchema(propertyKey, valueType, defaultValue, propertyState, dimension);
    }
}
