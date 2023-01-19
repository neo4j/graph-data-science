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
package org.neo4j.gds.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.api.DefaultValue.DEFAULT;
import static org.neo4j.gds.api.DefaultValue.DOUBLE_DEFAULT_FALLBACK;
import static org.neo4j.gds.api.DefaultValue.LONG_DEFAULT_FALLBACK;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class DefaultValueTest {

    @Test
    void shouldNotNestDefaultValues() {
        Assertions.assertEquals(DefaultValue.of(42), DefaultValue.of(DefaultValue.of(42)));
    }

    static Stream<Arguments> validLongValues() {
        return Stream.of(
            arguments(42, 42L),
            arguments(42L, 42L),
            arguments(42.0F, 42L),
            arguments(DefaultValue.FLOAT_DEFAULT_FALLBACK, LONG_DEFAULT_FALLBACK),
            arguments(42.0D, 42L),
            arguments(DefaultValue.DOUBLE_DEFAULT_FALLBACK, LONG_DEFAULT_FALLBACK),
            arguments(null, LONG_DEFAULT_FALLBACK)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.api.DefaultValueTest#validLongValues")
    void shouldReturnLongsFromNumericValues(Object input, long expected) {
        var defaultValue = DefaultValue.of(input);

        assertEquals(expected, defaultValue.longValue());
    }

    static Stream<Arguments> invalidNumericValues() {
        return Stream.of(
            arguments("Foo"),
            arguments(true),
            arguments(new long[]{1L, 2L})
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.api.DefaultValueTest#invalidNumericValues")
    void shouldThrowAnErrorIfValueCannotBeCoercedToLong(Object input) {
        var defaultValue = DefaultValue.of(input);

        assertThatThrownBy(defaultValue::longValue)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(formatWithLocale(
                "Expected type of default value to be `Long`. But got `%s`.",
                input.getClass().getSimpleName()
            ));
    }

    @Test
    void shouldThrowAnErrorIfDoubleCannotBeSafelyCoercedToLong() {
        var defaultValue = DefaultValue.of(42.42);

        var e = assertThrows(UnsupportedOperationException.class, defaultValue::longValue);

        assertThat(e.getMessage()).contains("Cannot safely convert 42.42 into an long value");
    }

    static Stream<Arguments> defaultValueArrayParams() {
        return Stream.of(
                Arguments.of(DefaultValue.of(new float[] {1337F})),
                Arguments.of(DefaultValue.of(new double[] {1337D})),
                Arguments.of(DefaultValue.of(new long[] {1337L}))
            );
    }

    @ParameterizedTest
    @MethodSource("defaultValueArrayParams")
    void shouldAcceptArrayAsDoubleArray(DefaultValue value) {
        assertThat(value.doubleArrayValue()).usingComparatorWithPrecision(1e-3).containsExactly(1337D);
    }

    @ParameterizedTest
    @MethodSource("defaultValueArrayParams")
    void shouldAcceptArrayAsFloatArray(DefaultValue value) {
        assertThat(value.floatArrayValue()).usingComparatorWithPrecision(1e-3F).containsExactly(1337F);
    }

    @ParameterizedTest
    @MethodSource("defaultValueArrayParams")
    void shouldAcceptArrayAsLongArray(DefaultValue value) {
        assertThat(value.longArrayValue()).containsExactly(1337L);
    }

    static Stream<Arguments> validDoubleValues() {
        return Stream.of(
            arguments(42, 42.0D),
            arguments(42L, 42.0D),
            arguments(LONG_DEFAULT_FALLBACK, DefaultValue.DOUBLE_DEFAULT_FALLBACK),
            arguments(42.123F, 42.123D),
            arguments(Float.MAX_VALUE, ((Float) Float.MAX_VALUE).doubleValue()),
            arguments(DefaultValue.FLOAT_DEFAULT_FALLBACK, DefaultValue.DOUBLE_DEFAULT_FALLBACK),
            arguments(42.4242D, 42.4242D),
            arguments(null, DefaultValue.DOUBLE_DEFAULT_FALLBACK)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.api.DefaultValueTest#validDoubleValues")
    void shouldReturnDoublesFromNumericValues(Object input, Double expected) {
        var defaultValue = DefaultValue.of(input);

        assertEquals(expected, defaultValue.doubleValue(), 0.01);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.api.DefaultValueTest#invalidNumericValues")
    void shouldThrowAnErrorIfValueCannotBeCoercedToDouble(Object input) {
        var defaultValue = DefaultValue.of(input);

        assertThatThrownBy(defaultValue::doubleValue)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(formatWithLocale(
                "Expected type of default value to be `Double`. But got `%s`.",
                input.getClass().getSimpleName()
            ));
    }

    @Test
    void shouldThrowAnErrorIfLongCannotBeSafelyCoercedToDouble() {
        var defaultValue = DefaultValue.of(1L << 53 + 1);

        var e = assertThrows(UnsupportedOperationException.class, defaultValue::doubleValue);

        assertThat(e.getMessage()).contains("Cannot safely convert");
    }

    @ParameterizedTest
    @MethodSource("valuesWithValueType")
    void shouldCreateDefaultValueUsingValueType(Object valueToSet, ValueType type, Function<DefaultValue, ?> fn, Object expectedValue) {
        var defaultValue = DefaultValue.of(valueToSet, type, true);
        assertThat(fn.apply(defaultValue)).isEqualTo(expectedValue);
    }

    @ParameterizedTest
    @MethodSource("values")
    void createUserDefinedValueTypes(Object valueToSet, Function<DefaultValue, ?> fn, Object expectedValue) {
        var defaultValue = DefaultValue.of(valueToSet, true);
        assertThat(fn.apply(defaultValue)).isEqualTo(expectedValue);
    }

    @Test
    void initFromValueType() {
        assertThat(DefaultValue.of(ValueType.DOUBLE)).isEqualTo(DefaultValue.forDouble());
        assertThat(DefaultValue.of(ValueType.LONG)).isEqualTo(DefaultValue.forLong());
        assertThat(DefaultValue.of(ValueType.LONG_ARRAY)).isEqualTo(DefaultValue.forLongArray());
        assertThat(DefaultValue.of(ValueType.FLOAT_ARRAY)).isEqualTo(DefaultValue.forFloatArray());
        assertThat(DefaultValue.of(ValueType.DOUBLE_ARRAY)).isEqualTo(DefaultValue.forDoubleArray());
        assertThat(DefaultValue.of(ValueType.STRING)).isEqualTo(DefaultValue.of(DEFAULT));
    }

    private static Stream<Arguments> values() {
        return Stream.of(
            Arguments.of(42, (Function<DefaultValue, ?>) DefaultValue::longValue, 42L),
            Arguments.of(42, (Function<DefaultValue, ?>) DefaultValue::doubleValue, 42D),
            Arguments.of(13.37, (Function<DefaultValue, ?>) DefaultValue::doubleValue, 13.37D),
            Arguments.of(
                List.of(13.37, 42),
                (Function<DefaultValue, ?>) DefaultValue::doubleArrayValue,
                new double[]{13.37D, 42D}
            ),
            Arguments.of(
                List.of(1337L, 42L),
                (Function<DefaultValue, ?>) DefaultValue::longArrayValue,
                new long[]{1337L, 42L}
            ),
            Arguments.of(
                List.of(1337, 42),
                (Function<DefaultValue, ?>) DefaultValue::longArrayValue,
                new long[]{1337L, 42L}
            )
        );
    }

    private static Stream<Arguments> valuesWithValueType() {
        return Stream.of(
            Arguments.of("42", ValueType.LONG, (Function<DefaultValue, ?>) DefaultValue::longValue, 42L),
            Arguments.of("42", ValueType.DOUBLE, (Function<DefaultValue, ?>) DefaultValue::doubleValue, 42D),
            Arguments.of("13.37", ValueType.DOUBLE, (Function<DefaultValue, ?>) DefaultValue::doubleValue, 13.37D),
            Arguments.of(List.of("13.37", "42"), ValueType.DOUBLE_ARRAY, (Function<DefaultValue, ?>) DefaultValue::doubleArrayValue, new double[] {13.37D, 42D}),
            Arguments.of(new String[]{"13.37", "42"}, ValueType.DOUBLE_ARRAY, (Function<DefaultValue, ?>) DefaultValue::doubleArrayValue, new double[] {13.37D, 42D}),
            Arguments.of(List.of("1337", "42"), ValueType.LONG_ARRAY, (Function<DefaultValue, ?>) DefaultValue::longArrayValue, new long[] {1337L, 42L}),
            Arguments.of(new String[]{"1337", "42"}, ValueType.LONG_ARRAY, (Function<DefaultValue, ?>) DefaultValue::longArrayValue, new long[] {1337L, 42L}),
            Arguments.of(List.of("13.37", "42"), ValueType.FLOAT_ARRAY, (Function<DefaultValue, ?>) DefaultValue::floatArrayValue, new float[] {13.37F, 42F}),
            Arguments.of(new String[]{"13.37", "42"}, ValueType.FLOAT_ARRAY, (Function<DefaultValue, ?>) DefaultValue::floatArrayValue, new float[] {13.37F, 42F}),
            Arguments.of("", ValueType.LONG, (Function<DefaultValue, ?>) DefaultValue::longValue, LONG_DEFAULT_FALLBACK),
            Arguments.of(null, ValueType.LONG, (Function<DefaultValue, ?>) DefaultValue::longValue, LONG_DEFAULT_FALLBACK),
            Arguments.of("", ValueType.DOUBLE, (Function<DefaultValue, ?>) DefaultValue::doubleValue, DOUBLE_DEFAULT_FALLBACK),
            Arguments.of(null, ValueType.DOUBLE, (Function<DefaultValue, ?>) DefaultValue::doubleValue, DOUBLE_DEFAULT_FALLBACK),
            Arguments.of("", ValueType.DOUBLE_ARRAY, (Function<DefaultValue, ?>) DefaultValue::doubleArrayValue, DEFAULT.doubleArrayValue()),
            Arguments.of(null, ValueType.DOUBLE_ARRAY, (Function<DefaultValue, ?>) DefaultValue::doubleArrayValue, DEFAULT.doubleArrayValue()),
            Arguments.of("", ValueType.LONG_ARRAY, (Function<DefaultValue, ?>) DefaultValue::longArrayValue, DEFAULT.longArrayValue()),
            Arguments.of(null, ValueType.LONG_ARRAY, (Function<DefaultValue, ?>) DefaultValue::longArrayValue, DEFAULT.longArrayValue()),
            Arguments.of("", ValueType.FLOAT_ARRAY, (Function<DefaultValue, ?>) DefaultValue::floatArrayValue, DEFAULT.floatArrayValue()),
            Arguments.of(null, ValueType.FLOAT_ARRAY, (Function<DefaultValue, ?>) DefaultValue::floatArrayValue, DEFAULT.floatArrayValue())
        );
    }
}
