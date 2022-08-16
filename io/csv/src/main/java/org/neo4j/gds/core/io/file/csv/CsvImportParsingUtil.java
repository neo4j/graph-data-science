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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Optional;

final class CsvImportParsingUtil {

    private static final EnumMap<ValueType, ParsingFunction> PARSING_FUNCTION_MAPPING = new EnumMap<>(ValueType.class);

    static {
        for (ValueType value : ValueType.values()) {
            parsingFunctionForValueType(value).map(parsingFunction -> PARSING_FUNCTION_MAPPING.put(value, parsingFunction));
        }
    }

    @FunctionalInterface
    interface ParsingFunction {
        Object fromCsvValue(DefaultValue defaultValue, JsonNode node);
    }

    static ParsingFunction getParsingFunction(ValueType valueType) {
        return PARSING_FUNCTION_MAPPING.get(valueType);
    }

    public static Object parse(
        String value,
        ValueType valueType,
        DefaultValue defaultValue,
        ObjectReader arrayReader
    ) throws IOException {
        switch (valueType) {
            case LONG:
                if (value.isBlank()) {
                    return defaultValue.longValue();
                }
                return Long.parseLong(value);
            case DOUBLE:
                if (value.isBlank()) {
                    return defaultValue.doubleValue();
                }
                return Double.parseDouble(value);
            case FLOAT_ARRAY:
                return readFloatArray(value, defaultValue, arrayReader);
            case DOUBLE_ARRAY:
                return readDoubleArray(value, defaultValue, arrayReader);
            case LONG_ARRAY:
                return readLongArray(value, defaultValue, arrayReader);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static float[] readFloatArray(
        String value,
        DefaultValue defaultValue,
        ObjectReader arrayReader
    ) throws IOException {
        MappingIterator<String[]> objectMappingIterator = arrayReader.readValues(value);
        if (objectMappingIterator.hasNext()) {
            var stringArray = objectMappingIterator.next();
            var parsedArray = new float[stringArray.length];
            for (int i = 0; i < stringArray.length; i++) {
                String s = stringArray[i];
                parsedArray[i] = Float.parseFloat(s);
            }
            return parsedArray;
        }
        return defaultValue.floatArrayValue();
    }

    private static double[] readDoubleArray(
        String value,
        DefaultValue defaultValue,
        ObjectReader arrayReader
    ) throws IOException {
        MappingIterator<String[]> objectMappingIterator = arrayReader.readValues(value);
        if (objectMappingIterator.hasNext()) {
            var stringArray = objectMappingIterator.next();
            var parsedArray = new double[stringArray.length];
            for (int i = 0; i < stringArray.length; i++) {
                String s = stringArray[i];
                parsedArray[i] = (double) parse(s, ValueType.DOUBLE, defaultValue, arrayReader);
            }
            return parsedArray;
        }
        return defaultValue.doubleArrayValue();
    }

    private static long[] readLongArray(
        String value,
        DefaultValue defaultValue,
        ObjectReader arrayReader
    ) throws IOException {
        MappingIterator<String[]> objectMappingIterator = arrayReader.readValues(value);
        if (objectMappingIterator.hasNext()) {
            var stringArray = objectMappingIterator.next();
            var parsedArray = new long[stringArray.length];
            for (int i = 0; i < stringArray.length; i++) {
                String s = stringArray[i];
                parsedArray[i] = (long) parse(s, ValueType.LONG, defaultValue, arrayReader);
            }
            return parsedArray;
        }
        return defaultValue.longArrayValue();
    }

    private static Optional<ParsingFunction> parsingFunctionForValueType(ValueType valueType) {
        switch (valueType) {
            case LONG:
                return Optional.of(CsvImportParsingUtil::parseLong);
            case DOUBLE:
                return Optional.of(CsvImportParsingUtil::parseDouble);
            case LONG_ARRAY:
                return Optional.of(CsvImportParsingUtil::parseLongArray);
            case DOUBLE_ARRAY:
                return Optional.of(CsvImportParsingUtil::parseDoubleArray);
            case FLOAT_ARRAY:
                return Optional.of(CsvImportParsingUtil::parseFloatArray);
            default:
                return Optional.empty();
        }
    }

    private static Object parseLong(DefaultValue defaultValue, JsonNode node) {
        if (node == null || node.textValue().isBlank()) {
            return defaultValue.longValue();
        }
        return node.asLong();
    }

    private static Object parseDouble(DefaultValue defaultValue, JsonNode node) {
        if (node == null || node.textValue().isBlank()) {
            return defaultValue.doubleValue();
        }
        return node.asDouble();
    }

    private static Object parseLongArray(DefaultValue defaultValue, JsonNode node) {
        if (node == null || node.isEmpty()) {
            return defaultValue.longArrayValue();
        }
        var arrayNode = (ArrayNode) node;
        var size = arrayNode.size();
        var longArray = new long[size];
        for (int i = 0; i < size; i++) {
            longArray[i] = arrayNode.get(i).asLong();
        }
        return longArray;
    }

    private static Object parseDoubleArray(DefaultValue defaultValue, JsonNode node) {
        if (node == null || node.isEmpty()) {
            return defaultValue.doubleArrayValue();
        }
        var arrayNode = (ArrayNode) node;
        var size = arrayNode.size();
        var doubleArray = new double[size];
        for (int i = 0; i < size; i++) {
            doubleArray[i] = arrayNode.get(i).asDouble();
        }
        return doubleArray;
    }

    private static Object parseFloatArray(DefaultValue defaultValue, JsonNode node) {
        if (node == null || node.isEmpty()) {
            return defaultValue.floatArrayValue();
        }
        var arrayNode = (ArrayNode) node;
        var size = arrayNode.size();
        var floatArray = new float[size];
        for (int i = 0; i < size; i++) {
            floatArray[i] = (float) arrayNode.get(i).asDouble();
        }
        return floatArray;

    }

    private CsvImportParsingUtil() {}
}
