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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.ValueConversion;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class Neo4jValueConversionTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.Neo4jValueConversionTest#longConversion")
    void testGettingALong(Value value, Long expected) {
        if (expected != null) {
            Assertions.assertEquals(expected, Neo4jValueConversion.getLongValue(value));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> Neo4jValueConversion.getLongValue(value));
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

    @Test
    void shouldConvertEmptyLongArrayToDoubleArray() {
        assertThat(Neo4jValueConversion.getDoubleArray(Values.longArray(new long[0]))).isEqualTo(new double[0]);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.Neo4jValueConversionTest#doubleConversion")
    void testGettingADouble(Value value, Double expected) {
        if (expected != null) {
            assertEquals(expected, Neo4jValueConversion.getDoubleValue(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> Neo4jValueConversion.getDoubleValue(value));
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

    static float getFloatValue(Value value) {
        if (value instanceof FloatingPointValue) {
            var doubleValue = ((FloatingPointValue) value).doubleValue();
            return ValueConversion.notOverflowingDoubleToFloat(doubleValue);
        } else if (value instanceof IntegralValue) {
            return ValueConversion.exactLongToFloat(((IntegralValue) value).longValue());
        } else {
            throw new UnsupportedOperationException("Failed to convert to float");
        }
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.Neo4jValueConversionTest#floatConversion")
    void testGettingAFloat(Value value, Float expected) {
        if (expected != null) {
            assertEquals(expected, getFloatValue(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> getFloatValue(value));
        }
    }

    static Stream<Arguments> floatConversion() {
        return Stream.of(
            arguments(Values.floatValue(42.1F), 42.1F),
            arguments(Values.doubleValue(42.1D), 42.1F),
            arguments(Values.longValue(42L), 42.0F),
            arguments(Values.intValue(42), 42.0F),
            arguments(Values.shortValue((short) 42), 42.0F),
            arguments(Values.byteValue((byte) 42), 42.0F),

            arguments(Values.stringValue("42L"), null),
            arguments(Values.doubleArray(new double[]{42.0}), null),
            arguments(Values.longArray(new long[]{42}), null),
            arguments(Values.floatArray(new float[]{42.0F}), null),
            arguments(Values.longValue(Long.MAX_VALUE), null),
            arguments(Values.longValue(Long.MIN_VALUE), null),
            arguments(Values.doubleValue(Float.MAX_VALUE * 2.0D), null),
            arguments(Values.doubleValue(-Float.MAX_VALUE * 2.0D), null)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.Neo4jValueConversionTest#longArrayConversion")
    void testGettingALongArray(Value value, long[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, Neo4jValueConversion.getLongArray(value));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> Neo4jValueConversion.getLongArray(value));
        }
    }

    static Stream<Arguments> longArrayConversion() {
        return Stream.of(
            arguments(Values.longArray(new long[]{42L}), new long[]{42L}),

            arguments(Values.intArray(new int[]{42}), null),
            arguments(Values.floatArray(new float[]{42.0F}), new long[]{42L}),
            arguments(Values.floatArray(new float[]{42.42F}), null),
            arguments(Values.doubleArray(new double[]{42.0}), new long[]{42L}),
            arguments(Values.doubleArray(new double[]{42.42d}), null),
            arguments(Values.stringValue("42L"), null)
        );
    }

    @Test
    void testLongArrayConversionErrorShowsCause() {
        var input = Values.floatArray(new float[] {42.0F, 13.37F, 256.0F});
        assertThatThrownBy(() -> Neo4jValueConversion.getLongArray(input))
            .hasMessage("Cannot safely convert FloatArray[42.0, 13.37, 256.0] into a Long Array." +
                        " Cannot safely convert 13.37 into an long value");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.Neo4jValueConversionTest#doubleArrayConversion")
    void testGettingADoubleArray(Value value, double[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, Neo4jValueConversion.getDoubleArray(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> Neo4jValueConversion.getDoubleArray(value));
        }
    }

    static Stream<Arguments> doubleArrayConversion() {
        return Stream.of(
            arguments(Values.doubleArray(new double[]{42.0}), new double[]{42.0}),
            arguments(Values.floatArray(new float[]{42.0F}), new double[]{42.0}),
            arguments(Values.longArray(new long[]{42}), new double[]{42}),
            arguments(Values.intArray(new int[]{42}), new double[]{42}),
            arguments(Values.longArray(new long[]{9007199254740993L}), null),
            arguments(Values.stringValue("42L"), null)
        );
    }

    @Test
    void testDoubleArrayConversionErrorShowsCause() {
        var input = Values.longArray(new long[] {42, 9007199254740993L, -100});
        assertThatThrownBy(() -> Neo4jValueConversion.getDoubleArray(input))
            .hasMessage("Cannot safely convert LongArray[42, 9007199254740993, -100] into a Double Array." +
                        " Cannot safely convert 9007199254740993 into an double value");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.Neo4jValueConversionTest#floatArrayConversion")
    void testGettingAFloatArray(Value value, float[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, Neo4jValueConversion.getFloatArray(value), 0.1f);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> Neo4jValueConversion.getFloatArray(value));
        }
    }

    static Stream<Arguments> floatArrayConversion() {
        return Stream.of(
            arguments(Values.floatArray(new float[]{42.0f}), new float[]{42.0f}),
            arguments(Values.doubleArray(new double[]{42.0}), new float[]{42.0f}),
            arguments(Values.doubleArray(new double[]{Double.MAX_VALUE}), null),
            arguments(Values.longArray(new long[]{42}), new float[]{42.0f}),
            arguments(Values.longArray(new long[]{9007199254740993L}), null),
            arguments(Values.stringValue("42L"), null)
        );
    }
}
