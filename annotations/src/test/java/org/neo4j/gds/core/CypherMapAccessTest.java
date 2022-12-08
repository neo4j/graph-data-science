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
package org.neo4j.gds.core;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CypherMapAccessTest {

    @ParameterizedTest
    @MethodSource("positiveRangeValidationParameters")
    void shouldValidateDoubleRange(double value, double min, double max, boolean minInclusive, boolean maxInclusive) {
        assertEquals(value, CypherMapAccess.validateDoubleRange("value", value, min, max, minInclusive, maxInclusive));
    }

    @ParameterizedTest
    @MethodSource("negativeRangeValidationParameters")
    void shouldThrowForInvalidDoubleRange(
        double value,
        double min,
        double max,
        boolean minInclusive,
        boolean maxInclusive
    ) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> CypherMapAccess.validateDoubleRange("value", value, min, max, minInclusive, maxInclusive)
        );

        assertEquals(String.format(
            Locale.ENGLISH,
            "Value for `value` was `%s`, but must be within the range %s%.2f, %.2f%s.",
            value,
            minInclusive ? "[" : "(",
            min,
            max,
            maxInclusive ? "]" : ")"
        ), ex.getMessage());
    }

    @ParameterizedTest
    @MethodSource("positiveRangeValidationParameters")
    void shouldValidateIntegerRange(int value, int min, int max, boolean minInclusive, boolean maxInclusive) {
        assertEquals(
            value,
            CypherMapAccess.validateIntegerRange("value", value, min, max, minInclusive, maxInclusive)
        );
    }

    @ParameterizedTest
    @MethodSource("negativeRangeValidationParameters")
    void shouldThrowForInvalidIntegerRange(int value, int min, int max, boolean minInclusive, boolean maxInclusive) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> CypherMapAccess.validateIntegerRange("value", value, min, max, minInclusive, maxInclusive)
        );

        assertEquals(String.format(
            Locale.ENGLISH,
            "Value for `value` was `%s`, but must be within the range %s%d, %d%s.",
            value,
            minInclusive ? "[" : "(",
            min,
            max,
            maxInclusive ? "]" : ")"
        ), ex.getMessage());
    }

    static Stream<Arguments> positiveRangeValidationParameters() {
        return Stream.of(
            Arguments.of(42, 42, 84, true, false),
            Arguments.of(84, 42, 84, false, true),
            Arguments.of(42, 42, 42, true, true)
        );
    }

    static Stream<Arguments> negativeRangeValidationParameters() {
        return Stream.of(
            Arguments.of(42, 42, 84, false, false),
            Arguments.of(84, 42, 84, false, false),
            Arguments.of(42, 42, 42, false, false),

            Arguments.of(21, 42, 84, true, false),
            Arguments.of(85, 42, 84, false, true)
        );
    }
}
