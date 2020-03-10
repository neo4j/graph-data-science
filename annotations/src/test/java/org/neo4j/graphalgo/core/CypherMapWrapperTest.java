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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CypherMapWrapperTest {

    @Test
    void testCastingFromNumberToDouble() {
        Map<String, Object> numberPrimitives = map(
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
        Map<String, Object> numberPrimitives = map(
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
    void shouldValidateDoubleRange(double value, double min, double max, boolean minInclusive, boolean maxInclusive) {
        assertEquals(value, CypherMapWrapper.validateDoubleRange("value", value, min, max, minInclusive, maxInclusive));
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
            () -> CypherMapWrapper.validateDoubleRange("value", value, min, max, minInclusive, maxInclusive)
        );

        assertEquals(String.format(
            Locale.ENGLISH,
            "Value for `value` must be within %s%.2f, %.2f%s.",
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
            CypherMapWrapper.validateIntegerRange("value", value, min, max, minInclusive, maxInclusive)
        );
    }

    @ParameterizedTest
    @MethodSource("negativeRangeValidationParameters")
    void shouldThrowForInvalidIntegerRange(int value, int min, int max, boolean minInclusive, boolean maxInclusive) {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> CypherMapWrapper.validateIntegerRange("value", value, min, max, minInclusive, maxInclusive)
        );

        assertEquals(String.format(
            Locale.ENGLISH,
            "Value for `value` must be within %s%d, %d%s.",
            minInclusive ? "[" : "(",
            min,
            max,
            maxInclusive ? "]" : ")"
        ), ex.getMessage());
    }

    static Stream<Arguments> requiredParams() {
        return Stream.of(
            arguments(
                map("foo", 42, "bar", -42),
                "A similar parameter exists: [foo]"
            ),
            arguments(
                map("foo", 42, "bar", -42, "foi", 1337),
                "Similar parameters exist: [foi, foo]"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("requiredParams")
    void testRequireWithSuggestions(Map<String, Object> map, String expectedMessage) {
        CypherMapWrapper config = CypherMapWrapper.create(map);

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> config.requireLong("fou")
        );
        assertEquals(
            String.format("No value specified for the mandatory configuration parameter `fou` (%s)", expectedMessage),
            error.getMessage()
        );
    }

    static Stream<Arguments> unexpectedParams() {
        return Stream.of(
            arguments(
                map("fou", 42),
                singleton("foo"),
                "key: fou (Did you mean [foo]?)"
            ),
            arguments(
                map("fou", 42, "foi", 1337),
                singleton("foo"),
                "keys: foi (Did you mean [foo]?), fou (Did you mean [foo]?)"
            ),
            arguments(
                map("fou", 42),
                asList("foo", "foa"),
                "key: fou (Did you mean one of [foa, foo]?)"
            ),
            arguments(
                map("fou", 42, "foi", 1337),
                asList("foo", "foa"),
                "keys: foi (Did you mean one of [foa, foo]?), fou (Did you mean one of [foa, foo]?)"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("unexpectedParams")
    void testUnexpectedKeyWithSuggestion(Map<String, Object> map, Collection<String> keys, String expectedMessage) {
        CypherMapWrapper config = CypherMapWrapper.create(map);

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> config.requireOnlyKeysFrom(keys)
        );
        assertEquals(
            String.format("Unexpected configuration %s", expectedMessage),
            error.getMessage()
        );
    }

    static Stream<Arguments> mutextParams() {
        return Stream.of(
            arguments(
                map("aaa", 42, "bbb", 1337, "xxx", 42),
                "Invalid key: [xxx]. This key cannot be used together with `aaa` and `bbb`."
            ),
            arguments(
                map("aaa", 42, "bbb", 1337, "xxx", 42, "yyy", 1337),
                "Invalid keys: [xxx, yyy]. Those keys cannot be used together with `aaa` and `bbb`."
            ),
            arguments(
                map("aaa", 42, "xxx", 42, "yyy", 1337),
                "Invalid key: [aaa]. This key cannot be used together with `xxx` and `yyy`."
            ),
            arguments(
                map("aaa", 42, "bb", 42),
                "Test: No value specified for the mandatory configuration parameter `bbb` (A similar parameter exists: [bb])"
            ),
            arguments(
                map("aaa", 42, "bb", 42, "bbbb", 42),
                "Test: No value specified for the mandatory configuration parameter `bbb` (Similar parameters exist: [bbbb, bb])"
            ),
            arguments(
                map("aaa", 42, "bb", 42, "bbbb", 42, "xx", 1337),
                "Test: No value specified for the mandatory configuration parameter `bbb` (Similar parameters exist: [bbbb, bb])"
            ),
            arguments(
                map("aaa", 42, "bb", 42, "bbbb", 42, "xxx", 1337),
                "Test: Specify either `aaa` and `bbb` or `xxx` and `yyy`."
            ),
            arguments(
                map("xxx", 42, "yy", 42),
                "Test: No value specified for the mandatory configuration parameter `yyy` (A similar parameter exists: [yy])"
            ),
            arguments(
                map("xxx", 42, "yy", 42, "yyyy", 42),
                "Test: No value specified for the mandatory configuration parameter `yyy` (Similar parameters exist: [yyyy, yy])"
            ),
            arguments(
                map("xxx", 42, "yy", 42, "yyyy", 42, "aa", 1337),
                "Test: No value specified for the mandatory configuration parameter `yyy` (Similar parameters exist: [yyyy, yy])"
            ),
            arguments(
                map("xxx", 42, "yy", 42, "yyyy", 42, "aaa", 1337),
                "Test: Specify either `aaa` and `bbb` or `xxx` and `yyy`."
            ),
            arguments(
                map(),
                "Test: Specify either `aaa` and `bbb` or `xxx` and `yyy`."
            ),
            arguments(
                map("eee", 42, "fff", 1337),
                "Test: Specify either `aaa` and `bbb` or `xxx` and `yyy`."
            )
        );
    }

    @ParameterizedTest
    @MethodSource("mutextParams")
    void testMutexPairsConflictWithSecondPair(Map<String, Object> map, String expectedMessage) {
        CypherMapWrapper config = CypherMapWrapper.create(map);

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> config.verifyMutuallyExclusivePairs(
                "aaa", "bbb",
                "xxx", "yyy",
                "Test:"
            )
        );
        assertEquals(expectedMessage, error.getMessage());
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

    private static Map<String, Object> map(Object... objects) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < objects.length; ) {
            map.put((String) objects[i++], objects[i++]);
        }
        return map;
    }
}
