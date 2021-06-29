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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class DefaultValueUtil {

    private DefaultValueUtil() {}

    static Object transformObjectToPrimitiveArray(Object[] defaultArrayValue) {
        if (defaultArrayValue.length == 0) {
            return defaultArrayValue;
        }
        var firstEntry = defaultArrayValue[0];

        if (firstEntry instanceof Double) {
            return parseDoubleArrayValue(defaultArrayValue, ValueType.DOUBLE_ARRAY);
        } else if (firstEntry instanceof Float) {
            return parseFloatArrayValue(defaultArrayValue, ValueType.FLOAT_ARRAY);
        } else if (firstEntry instanceof Long || firstEntry instanceof Integer) {
            return parseLongArrayValue(defaultArrayValue, ValueType.LONG_ARRAY);
        } else {
            throw new IllegalStateException("Unexpected type of array " + firstEntry.getClass().getSimpleName());
        }
    }

    static double[] parseDoubleArrayValue(Object defaultValue, ValueType type) {
        double[] defaultDoubleArray;
        if (defaultValue instanceof Collection) {
            defaultDoubleArray = ((Collection<?>) defaultValue).stream()
                .map(Object::toString)
                .mapToDouble(Double::parseDouble)
                .toArray();
        } else if (defaultValue instanceof Object[]) {
            var objectArray = (Object[]) defaultValue;
            defaultDoubleArray = Arrays.stream(objectArray)
                .map(Object::toString)
                .mapToDouble(Double::parseDouble)
                .toArray();
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Cannot create default value of type `%s` from input value %s",
                type.toString(),
                defaultValue.getClass().getSimpleName()
            ));
        }
        return defaultDoubleArray;
    }

    static long[] parseLongArrayValue(Object defaultValue, ValueType type) {
        long[] defaultLongArray;
        if (defaultValue instanceof Collection) {
            defaultLongArray = ((Collection<?>) defaultValue).stream()
                .map(Object::toString)
                .mapToLong(Long::parseLong)
                .toArray();
        } else if (defaultValue instanceof Object[]) {
            var objectArray = (Object[]) defaultValue;
            defaultLongArray = Arrays.stream(objectArray)
                .map(Object::toString)
                .mapToLong(Long::parseLong)
                .toArray();
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Cannot create default value of type `%s` from input value %s",
                type.toString(),
                defaultValue.getClass().getSimpleName()
            ));
        }
        return defaultLongArray;
    }

    static float[] parseFloatArrayValue(Object defaultValue, ValueType type) {
        float[] defaultFloatArray;
        if (defaultValue instanceof List) {
            var df = ((List<?>) defaultValue);
            defaultFloatArray = new float[df.size()];
            for (int i = 0; i < df.size(); i++) {
                defaultFloatArray[i] = Float.parseFloat(df.get(i).toString());
            }
        } else if (defaultValue instanceof Object[]) {
            var objectArray = (Object[]) defaultValue;
            defaultFloatArray = new float[objectArray.length];
            for (int i = 0; i < objectArray.length; i++) {
                defaultFloatArray[i] = Float.parseFloat(objectArray[i].toString());
            }
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Cannot create default value of type `%s` from input value %s",
                type.toString(),
                defaultValue.getClass().getSimpleName()
            ));
        }
        return defaultFloatArray;
    }

}
