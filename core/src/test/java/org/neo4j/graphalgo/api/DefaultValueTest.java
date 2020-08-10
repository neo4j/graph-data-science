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
package org.neo4j.graphalgo.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class DefaultValueTest {

    @Test
    void shouldNotNestDefaultValues() {
        assertEquals(DefaultValue.of(42), DefaultValue.of(DefaultValue.of(42)));
    }

    static Stream<Arguments> validLongValues() {
        return Stream.of(
            arguments(42, 42L),
            arguments(42L, 42L),
            arguments(42.0F, 42L),
            arguments(DefaultValue.FLOAT_DEFAULT_FALLBACK, DefaultValue.LONG_DEFAULT_FALLBACK),
            arguments(42.0D, 42L),
            arguments(DefaultValue.DOUBLE_DEFAULT_FALLBACK, DefaultValue.LONG_DEFAULT_FALLBACK),
            arguments(null, DefaultValue.LONG_DEFAULT_FALLBACK)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.api.DefaultValueTest#validLongValues")
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
    @MethodSource("org.neo4j.graphalgo.api.DefaultValueTest#invalidNumericValues")
    void shouldThrowAnErrorIfValueCannotBeCoercedToLong(Object input) {
        var defaultValue = DefaultValue.of(input);

        var e = assertThrows(ClassCastException.class, defaultValue::longValue);

        assertThat(e.getMessage(), containsString(formatWithLocale("The default value %s cannot coerced into type Long.", defaultValue.getObject())));
    }

    @Test
    void shouldThrowAnErrorIfDoubleCannotBeSafelyCoercedToLong() {
        var defaultValue = DefaultValue.of(42.42);

        var e = assertThrows(UnsupportedOperationException.class, defaultValue::longValue);

        assertThat(e.getMessage(), containsString("Cannot safely convert 42.42 into an long value"));
    }

    static Stream<Arguments> validDoubleValues() {
        return Stream.of(
            arguments(42, 42.0D),
            arguments(42L, 42.0D),
            arguments(DefaultValue.LONG_DEFAULT_FALLBACK, DefaultValue.DOUBLE_DEFAULT_FALLBACK),
            arguments(42.123F, 42.123D),
            arguments(Float.MAX_VALUE, ((Float) Float.MAX_VALUE).doubleValue()),
            arguments(DefaultValue.FLOAT_DEFAULT_FALLBACK, DefaultValue.DOUBLE_DEFAULT_FALLBACK),
            arguments(42.4242D, 42.4242D),
            arguments(null, DefaultValue.DOUBLE_DEFAULT_FALLBACK)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.api.DefaultValueTest#validDoubleValues")
    void shouldReturnDoublesFromNumericValues(Object input, Double expected) {
        var defaultValue = DefaultValue.of(input);

        assertEquals(expected, defaultValue.doubleValue(), 0.01);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.api.DefaultValueTest#invalidNumericValues")
    void shouldThrowAnErrorIfValueCannotBeCoercedToDouble(Object input) {
        var defaultValue = DefaultValue.of(input);

        var e = assertThrows(ClassCastException.class, defaultValue::doubleValue);

        assertThat(e.getMessage(), containsString(formatWithLocale("The default value %s cannot coerced into type Double.", defaultValue.getObject())));
    }

    @Test
    void shouldThrowAnErrorIfLongCannotBeSafelyCoercedToDouble() {
        var defaultValue = DefaultValue.of(1L << 53 + 1);

        var e = assertThrows(UnsupportedOperationException.class, defaultValue::doubleValue);

        assertThat(e.getMessage(), containsString("Cannot safely convert"));
    }
}
