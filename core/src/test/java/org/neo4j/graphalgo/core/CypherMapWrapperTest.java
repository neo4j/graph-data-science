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

package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.compat.MapUtil;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CypherMapWrapperTest {

    @Test
    void testCastingFromNumberToDouble() {
        Map<String, Object> numberPrimitives = MapUtil.map(
            "integer", 42,
            "long", 1337L,
            "float", 1337.42f
        );
        CypherMapWrapper primitivesWrapper = CypherMapWrapper.create(numberPrimitives);
        assertEquals(42D, primitivesWrapper.getDouble("integer", 0.0D));
        assertEquals(1337D, primitivesWrapper.getDouble("long", 0.0D));
        assertEquals(1337.42D, primitivesWrapper.getDouble("float", 0.0D), 0.0001D);
    }

    @Test
    void shouldFailOnLossyCasts() {
        Map<String, Object> numberPrimitives = MapUtil.map(
            "double", 1337.42D
        );
        CypherMapWrapper primitivesWrapper = CypherMapWrapper.create(numberPrimitives);

        IllegalArgumentException doubleEx = assertThrows(
            IllegalArgumentException.class,
            () -> primitivesWrapper.getLong("double", 0)
        );

        assertTrue(doubleEx.getMessage().contains("must be of type `Long` but was `Double`"));
    }

    @ParameterizedTest
    @MethodSource("positiveRangeValidationParameters")
    void shouldValidateRange(double value, double min, double max, boolean minInclusive, boolean maxInclusive) {
        assertEquals(value, CypherMapWrapper.validateRange("value", value, min, max, minInclusive, maxInclusive));
    }

    @ParameterizedTest
    @MethodSource("negativeRangeValidationParameters")
    void shouldThrowForInvalidRange(double value, double min, double max, boolean minInclusive, boolean maxInclusive) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> CypherMapWrapper.validateRange("value", value, min, max, minInclusive, maxInclusive)
        );

        assertEquals(String.format("Value for `value` must be within %s%.2f, %.2f%s.",
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