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
package org.neo4j.gds.api.nodeproperties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.neo4j.gds.api.DefaultValue;

public enum ValueType {
    LONG {
        @Override
        public String cypherName() {
            return "Integer";
        }

        @Override
        public String csvName() {
            return "long";
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            if (node == null || node.textValue().isBlank()) {
                return fallbackValue.longValue();
            }
            return node.asLong();
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forLong();
        }
    },
    DOUBLE {
        @Override
        public String cypherName() {
            return "Float";
        }

        @Override
        public String csvName() {
            return "double";
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            if (node == null || node.textValue().isBlank()) {
                return fallbackValue.doubleValue();
            }
            return node.asDouble();
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forDouble();
        }
    },
    STRING {
        @Override
        public String cypherName() {
            return "String";
        }

        @Override
        public String csvName() {
            return "string";
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            throw new UnsupportedOperationException("Unsupported conversion from CSV value to STRING");
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.DEFAULT;
        }
    },
    DOUBLE_ARRAY {
        @Override
        public String cypherName() {
            return "List of Float";
        }

        @Override
        public String csvName() {
            return "double[]";
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            if (node == null || node.isEmpty()) {
                return fallbackValue.doubleArrayValue();
            }
            var arrayNode = (ArrayNode) node;
            var size = arrayNode.size();
            var doubleArray = new double[size];
            for (int i = 0; i < size; i++) {
                doubleArray[i] = arrayNode.get(i).asDouble();
            }
            return doubleArray;
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forDoubleArray();
        }
    },
    FLOAT_ARRAY {
        @Override
        public String cypherName() {
            return "List of Float";
        }

        @Override
        public String csvName() {
            return "float[]";
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            if (node == null || node.isEmpty()) {
                return fallbackValue.floatArrayValue();
            }
            var arrayNode = (ArrayNode) node;
            var size = arrayNode.size();
            var floatArray = new float[size];
            for (int i = 0; i < size; i++) {
                floatArray[i] = (float) arrayNode.get(i).asDouble();
            }
            return floatArray;
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forFloatArray();
        }
    },
    LONG_ARRAY {
        @Override
        public String cypherName() {
            return "List of Integer";
        }

        @Override
        public String csvName() {
            return "long[]";
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            if (node == null || node.isEmpty()) {
                return fallbackValue.longArrayValue();
            }
            var arrayNode = (ArrayNode) node;
            var size = arrayNode.size();
            var longArray = new long[size];
            for (int i = 0; i < size; i++) {
                longArray[i] = arrayNode.get(i).asLong();
            }
            return longArray;
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.forLongArray();
        }
    },
    UNKNOWN {
        @Override
        public String cypherName() {
            return "Unknown";
        }

        @Override
        public String csvName() {
            throw new UnsupportedOperationException("Value Type UKNONWN is not supported in CSV");
        }

        @Override
        public Object fromCsvValue(DefaultValue fallbackValue, JsonNode node) {
            throw new UnsupportedOperationException("Unsupported conversion from CSV value to UNKNOWN");
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.DEFAULT;
        }
    };

    public abstract String cypherName();

    public abstract String csvName();

    public abstract Object fromCsvValue(DefaultValue fallbackValue, JsonNode node);

    public abstract DefaultValue fallbackValue();

    public Object fromCsvValue(JsonNode node) {
        return fromCsvValue(fallbackValue(), node);
    }

    public static ValueType fromCsvName(String csvName) {
        for (ValueType value : values()) {
            if (value == UNKNOWN) {
                continue;
            }
            if (value.csvName().equals(csvName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unexpected value: " + csvName);
    }
}
