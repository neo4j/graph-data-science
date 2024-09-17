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
import org.neo4j.gds.values.FloatingPointValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.IntegralValue;
import org.neo4j.gds.values.primitive.PrimitiveValues;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class GdsNeo4jValueConversionTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.GdsNeo4jValueConversionTest#longConversion")
    void testGettingALong(GdsValue value, Long expected) {
        if (expected != null) {
            Assertions.assertEquals(expected, GdsNeo4jValueConversion.getLongValue(value));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> GdsNeo4jValueConversion.getLongValue(value));
        }
    }

    static Stream<Arguments> longConversion() {
        return Stream.of(
            arguments(PrimitiveValues.longValue(42L), 42L),
            arguments(PrimitiveValues.longValue(42), 42L),
            arguments(PrimitiveValues.longValue((short) 42), 42L),
            arguments(PrimitiveValues.longValue((byte) 42), 42L),
            arguments(PrimitiveValues.floatingPointValue(42.0F), 42L),
            arguments(PrimitiveValues.floatingPointValue(42.0), 42L),

            arguments(PrimitiveValues.longArray(new long[]{42L}), null),
            arguments(PrimitiveValues.floatingPointValue(42.12F), null),
            arguments(PrimitiveValues.floatingPointValue(42.12), null)
        );
    }

    @Test
    void shouldConvertEmptyLongArrayToDoubleArray() {
        assertThat(GdsNeo4jValueConversion.getDoubleArray(PrimitiveValues.longArray(new long[0]))).isEqualTo(new double[0]);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.GdsNeo4jValueConversionTest#doubleConversion")
    void testGettingADouble(GdsValue value, Double expected) {
        if (expected != null) {
            assertEquals(expected, GdsNeo4jValueConversion.getDoubleValue(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> GdsNeo4jValueConversion.getDoubleValue(value));
        }
    }

    static Stream<Arguments> doubleConversion() {
        return Stream.of(
            arguments(PrimitiveValues.floatingPointValue(42.1), 42.1D),
            arguments(PrimitiveValues.floatingPointValue(42.1F), 42.1D),
            arguments(PrimitiveValues.longValue(42L), 42.0D),
            arguments(PrimitiveValues.longValue(42), 42.0D),
            arguments(PrimitiveValues.longValue((short) 42), 42.0D),
            arguments(PrimitiveValues.longValue((byte) 42), 42.0D),

            arguments(PrimitiveValues.doubleArray(new double[]{42.0}), null),
            arguments(PrimitiveValues.longValue(1L << 54 + 1), null)
        );
    }

    static float getFloatValue(GdsValue value) {
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
    @MethodSource("org.neo4j.gds.utils.GdsNeo4jValueConversionTest#floatConversion")
    void testGettingAFloat(GdsValue value, Float expected) {
        if (expected != null) {
            assertEquals(expected, getFloatValue(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> getFloatValue(value));
        }
    }

    static Stream<Arguments> floatConversion() {
        return Stream.of(
            arguments(PrimitiveValues.floatingPointValue(42.1F), 42.1F),
            arguments(PrimitiveValues.floatingPointValue(42.1D), 42.1F),
            arguments(PrimitiveValues.longValue(42L), 42.0F),
            arguments(PrimitiveValues.longValue(42), 42.0F),
            arguments(PrimitiveValues.longValue((short) 42), 42.0F),
            arguments(PrimitiveValues.longValue((byte) 42), 42.0F),

            arguments(PrimitiveValues.doubleArray(new double[]{42.0}), null),
            arguments(PrimitiveValues.longArray(new long[]{42}), null),
            arguments(PrimitiveValues.floatArray(new float[]{42.0F}), null),
            arguments(PrimitiveValues.longValue(Long.MAX_VALUE), null),
            arguments(PrimitiveValues.longValue(Long.MIN_VALUE), null),
            arguments(PrimitiveValues.floatingPointValue(Float.MAX_VALUE * 2.0D), null),
            arguments(PrimitiveValues.floatingPointValue(-Float.MAX_VALUE * 2.0D), null)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.GdsNeo4jValueConversionTest#longArrayConversion")
    void testGettingALongArray(GdsValue value, long[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, GdsNeo4jValueConversion.getLongArray(value));
        } else {
            assertThrows(UnsupportedOperationException.class, () -> GdsNeo4jValueConversion.getLongArray(value));
        }
    }

    static Stream<Arguments> longArrayConversion() {
        return Stream.of(
            arguments(PrimitiveValues.longArray(new long[]{42L}), new long[]{42L}),
            arguments(PrimitiveValues.intArray(new int[]{42}), new long[]{42L}),
            arguments(PrimitiveValues.floatArray(new float[]{42.0F}), new long[]{42L}),
            arguments(PrimitiveValues.floatArray(new float[]{42.42F}), null),
            arguments(PrimitiveValues.doubleArray(new double[]{42.0}), new long[]{42L}),
            arguments(PrimitiveValues.doubleArray(new double[]{42.42d}), null)
        );
    }

    @Test
    void testLongArrayConversionErrorShowsCause() {
        var input = PrimitiveValues.floatArray(new float[] {42.0F, 13.37F, 256.0F});
        assertThatThrownBy(() -> GdsNeo4jValueConversion.getLongArray(input))
            .hasMessage("Cannot safely convert FloatArray[42.0, 13.37, 256.0] into a Long Array." +
                        " Cannot safely convert 13.37 into an long value");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.GdsNeo4jValueConversionTest#doubleArrayConversion")
    void testGettingADoubleArray(GdsValue value, double[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, GdsNeo4jValueConversion.getDoubleArray(value), 0.1);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> GdsNeo4jValueConversion.getDoubleArray(value));
        }
    }

    static Stream<Arguments> doubleArrayConversion() {
        return Stream.of(
            arguments(PrimitiveValues.doubleArray(new double[]{42.0}), new double[]{42.0}),
            arguments(PrimitiveValues.floatArray(new float[]{42.0F}), new double[]{42.0}),
            arguments(PrimitiveValues.longArray(new long[]{42}), new double[]{42}),
            arguments(PrimitiveValues.intArray(new int[]{42}), new double[]{42}),
            arguments(PrimitiveValues.longArray(new long[]{9007199254740993L}), null)
        );
    }

    @Test
    void testDoubleArrayConversionErrorShowsCause() {
        var input = PrimitiveValues.longArray(new long[] {42, 9007199254740993L, -100});
        assertThatThrownBy(() -> GdsNeo4jValueConversion.getDoubleArray(input))
            .hasMessage("Cannot safely convert LongArray[42, 9007199254740993, -100] into a Double Array." +
                        " Cannot safely convert 9007199254740993 into an double value");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.utils.GdsNeo4jValueConversionTest#floatArrayConversion")
    void testGettingAFloatArray(GdsValue value, float[] expected) {
        if (expected != null) {
            assertArrayEquals(expected, GdsNeo4jValueConversion.getFloatArray(value), 0.1f);
        } else {
            assertThrows(UnsupportedOperationException.class, () -> GdsNeo4jValueConversion.getFloatArray(value));
        }
    }

    static Stream<Arguments> floatArrayConversion() {
        return Stream.of(
            arguments(PrimitiveValues.floatArray(new float[]{42.0f}), new float[]{42.0f}),
            arguments(PrimitiveValues.doubleArray(new double[]{42.0}), new float[]{42.0f}),
            arguments(PrimitiveValues.doubleArray(new double[]{Double.MAX_VALUE}), null),
            arguments(PrimitiveValues.longArray(new long[]{42}), new float[]{42.0f}),
            arguments(PrimitiveValues.longArray(new long[]{9007199254740993L}), null)
        );
    }
}
