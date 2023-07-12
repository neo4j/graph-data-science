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
package org.neo4j.gds.core.io.file.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;

final class DefaultValueIOHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_VALUE_PREFIX = "DefaultValue(";

    private DefaultValueIOHelper() {}

    static String serialize(DefaultValue defaultValue) {
        try {
            var serializedValue = OBJECT_MAPPER.writeValueAsString(defaultValue.getObject());
            return DEFAULT_VALUE_PREFIX + serializedValue + ")";
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    static DefaultValue deserialize(String value, ValueType valueType, boolean isUserDefined) {
        try {
            if (value == null || value.isEmpty()) {
                return valueType.fallbackValue();
            }
            Object parseValue;
            switch (valueType) {
                case DOUBLE:
                    parseValue = OBJECT_MAPPER.readValue(value, double.class);
                    break;
                case LONG:
                    parseValue = OBJECT_MAPPER.readValue(value, long.class);
                    break;
                case LONG_ARRAY:
                    parseValue = OBJECT_MAPPER.readValue(value, long[].class);
                    break;
                case FLOAT_ARRAY:
                    parseValue = OBJECT_MAPPER.readValue(value, float[].class);
                    break;
                case DOUBLE_ARRAY:
                    parseValue = OBJECT_MAPPER.readValue(value, double[].class);
                    break;
                default:
                    throw new IllegalArgumentException("Cannot deserialize type `" + valueType + "` to DefaultValue");
            }
            return DefaultValue.of(parseValue, valueType, isUserDefined);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
