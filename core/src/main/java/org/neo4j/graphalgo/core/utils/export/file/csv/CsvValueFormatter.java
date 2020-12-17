/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.api.DefaultValue.INTEGER_DEFAULT_FALLBACK;
import static org.neo4j.graphalgo.api.DefaultValue.LONG_DEFAULT_FALLBACK;

public final class CsvValueFormatter {
    private CsvValueFormatter() {}

    public static String format(Object value) {
        if (value == null) {
            return "";
        }

        if (value instanceof Long) {
            long longValue = (Long) value;
            if (longValue == LONG_DEFAULT_FALLBACK || longValue == INTEGER_DEFAULT_FALLBACK) {
                return "";
            }
            return Long.toString(longValue);
        } else if (value instanceof Integer) {
            var integerValue = (int) value;
            if (integerValue == INTEGER_DEFAULT_FALLBACK) {
                return "";
            }
            return Integer.toString(integerValue);
        } else if (value instanceof Double) {
            var doubleValue = (Double) value;
            if (doubleValue.isNaN()) {
                return "";
            }
            return doubleValue.toString();
        } else if (value instanceof Float) {
            var floatValue = (Float) value;
            if (floatValue.isNaN()) {
                return "";
            }
            return floatValue.toString();
        } else if (value instanceof double[]) {
            var doubleArray = (double[]) value;
            return Arrays.stream(doubleArray).mapToObj(Double::toString).collect(Collectors.joining(";"));
        } else if (value instanceof float[]) {
            var floatArray = (float[]) value;
            return IntStream
                .range(0, floatArray.length)
                .mapToDouble(i -> floatArray[i])
                .mapToObj(Double::toString)
                .collect(Collectors.joining(";"));
        } else if (value instanceof long[]) {
            var longArray = (long[]) value;
            return Arrays.stream(longArray).mapToObj(Long::toString).collect(Collectors.joining(";"));

        } else {
            return value.toString();
        }
    }
}
