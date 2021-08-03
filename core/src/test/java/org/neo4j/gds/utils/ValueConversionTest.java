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
package org.neo4j.gds.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ValueConversionTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.utils.ValueConversionTest#longConversion")
    void testGettingALong(Value value, Long expected) {
        if (expected != null) {
            Assertions.assertEquals(expected, ValueConversion.getLongValue(value));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> ValueConversion.getLongValue(value));
        }
    }

    static Stream<Arguments> longConversion() {
        return Stream.of(
            arguments(Values.longValue(42L), 42L),
            arguments(Values.intValue(42), 42L),
            arguments(Values.shortValue((short) 42), 42L),
            arguments(Values.byteValue((byte) 42), 42L),
            arguments(Values.floatValue(42.0F), 42L),
            arguments(Values.doubleValue(42.0), 42L),

            arguments(Values.stringValue("42L"), null),
            arguments(Values.longArray(new long[]{42L}), null),
            arguments(Values.floatValue(42.12F), null),
            arguments(Values.doubleValue(42.12), null)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.utils.ValueConversionTest#doubleConversion")
    void testGettingADouble(Value value, Double expected) {
        if (expected != null) {
            assertEquals(expected, ValueConversion.getDoubleValue(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> ValueConversion.getDoubleValue(value));
        }
    }

    static Stream<Arguments> doubleConversion() {
        return Stream.of(
            arguments(Values.doubleValue(42.1), 42.1D),
            arguments(Values.floatValue(42.1F), 42.1D),
            arguments(Values.longValue(42L), 42.0D),
            arguments(Values.intValue(42), 42.0D),
            arguments(Values.shortValue((short) 42), 42.0D),
            arguments(Values.byteValue((byte) 42), 42.0D),

            arguments(Values.stringValue("42L"), null),
            arguments(Values.doubleArray(new double[]{42.0}), null),
            arguments(Values.longValue(1L << 54 + 1), null)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.utils.ValueConversionTest#longArrayConversion")
    void testGettingALongArray(Value value, long[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, ValueConversion.getLongArray(value));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> ValueConversion.getLongArray(value));
        }
    }

    static Stream<Arguments> longArrayConversion() {
        return Stream.of(
            arguments(Values.longArray(new long[]{42L}), new long[]{42L}),

            arguments(Values.intArray(new int[]{42}), null),
            arguments(Values.floatArray(new float[]{42.0F}), null),
            arguments(Values.stringValue("42L"), null)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.utils.ValueConversionTest#doubleArrayConversion")
    void testGettingADoubleArray(Value value, double[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, ValueConversion.getDoubleArray(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> ValueConversion.getDoubleArray(value));
        }
    }

    static Stream<Arguments> doubleArrayConversion() {
        return Stream.of(
            arguments(Values.doubleArray(new double[]{42.0}), new double[]{42.0}),

            arguments(Values.floatArray(new float[]{42.0F}), null),
            arguments(Values.longArray(new long[]{42}), null),
            arguments(Values.stringValue("42L"), null)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.utils.ValueConversionTest#floatArrayConversion")
    void testGettingAFloatArray(Value value, float[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, ValueConversion.getFloatArray(value), 0.1f);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> ValueConversion.getFloatArray(value));
        }
    }

    static Stream<Arguments> floatArrayConversion() {
        return Stream.of(
            arguments(Values.floatArray(new float[]{42.0f}), new float[]{42.0f}),

            arguments(Values.doubleArray(new double[]{42.0}), null),
            arguments(Values.longArray(new long[]{42}), null),
            arguments(Values.stringValue("42L"), null)
        );
    }
}
