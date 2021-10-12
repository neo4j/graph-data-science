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

import org.neo4j.gds.api.DefaultValue;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        public String csvValue(Object value) {
            if (value == null || (Long) value == DefaultValue.LONG_DEFAULT_FALLBACK || (Long) value == DefaultValue.INTEGER_DEFAULT_FALLBACK) {
                return "";
            }
            return value.toString();
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
            if (csvValue.isBlank()) {
                return fallbackValue.longValue();
            }
            return Long.parseLong(csvValue);
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
        public String csvValue(Object value) {
            if (value == null || ((Double) value).isNaN()) {
                return "";
            }
            return value.toString();
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
            if (csvValue.isBlank()) {
                return fallbackValue.doubleValue();
            }
            return Double.parseDouble(csvValue);
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
        public String csvValue(Object value) {
            if (value == null) {
                return "";
            }
            return value.toString();
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
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
        public String csvValue(Object value) {
            if (value == null) {
                return "";
            }
            var doubleArray = (double[]) value;
            return Arrays.stream(doubleArray).mapToObj(Double::toString).collect(Collectors.joining(";"));
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
            if (csvValue.isBlank()) {
                return fallbackValue.doubleArrayValue();
            }
            String[] arrayElements = csvValue.split(";");
            double[] doubleArray = new double[arrayElements.length];
            for (int i = 0; i < arrayElements.length; i++) {
                doubleArray[i] = Double.parseDouble(arrayElements[i]);
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
        public String csvValue(Object value) {
            if (value == null) {
                return "";
            }
            var floatArray = (float[]) value;
            return IntStream
                .range(0, floatArray.length)
                .mapToDouble(i -> floatArray[i])
                .mapToObj(Double::toString)
                .collect(Collectors.joining(";"));
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
            if (csvValue.isBlank()) {
                return fallbackValue.floatArrayValue();
            }
            String[] arrayElements = csvValue.split(";");
            float[] floatArray = new float[arrayElements.length];
            for (int i = 0; i < arrayElements.length; i++) {
                floatArray[i] = Float.parseFloat(arrayElements[i]);
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
        public String csvValue(Object value) {
            if (value == null) {
                return "";
            }
            var longArray = (long[]) value;
            return Arrays.stream(longArray).mapToObj(Long::toString).collect(Collectors.joining(";"));
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
            if (csvValue.isBlank()) {
                return fallbackValue.longArrayValue();
            }
            String[] arrayElements = csvValue.split(";");
            long[] longArray = new long[arrayElements.length];
            for (int i = 0; i < arrayElements.length; i++) {
                longArray[i] = Long.parseLong(arrayElements[i]);
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
        public String csvValue(Object value) {
            if (value == null) {
                return "";
            }
            return value.toString();
        }

        @Override
        public Object fromCsvValue(String csvValue, DefaultValue fallbackValue) {
            throw new UnsupportedOperationException("Unsupported conversion from CSV value to UNKNOWN");
        }

        @Override
        public DefaultValue fallbackValue() {
            return DefaultValue.DEFAULT;
        }
    };

    public abstract String cypherName();

    public abstract String csvName();

    public abstract String csvValue(Object value);

    public abstract Object fromCsvValue(String csvValue, DefaultValue fallbackValue);

    public abstract DefaultValue fallbackValue();

    public Object fromCsvValue(String csvValue) {
        return fromCsvValue(csvValue, fallbackValue());
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
