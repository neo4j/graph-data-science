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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CypherMapWrapperTest {

    @Test
    void shouldConvertOptional() {
        CypherMapWrapper wrapper = CypherMapWrapper.create(Map.of("foo", 1L, "bar", "actualBar"));

        assertThat(wrapper.getOptional("foo", Long.class)).contains(1L);
        assertThat(wrapper.getOptional("foo", Integer.class)).contains(1);
        assertThat(wrapper.getOptional("bar", String.class)).contains("actualBar");
        assertThat(wrapper.getOptional("baz", String.class)).isEmpty();
    }

    @Test
    void failsWhenLongIsTooLarge() {
        long tooLarge = Integer.MAX_VALUE + 1L;
        CypherMapWrapper wrapper = CypherMapWrapper.create(Map.of("foo", tooLarge));

        assertThat(wrapper.getOptional("foo", Long.class)).contains(tooLarge);
        assertThrows(ArithmeticException.class, () -> wrapper.getOptional("foo", Integer.class));
    }

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

    @ParameterizedTest
    @ValueSource(strings = {
            "nodeProperty",
            "nodeproperty",
            "NodeProperty",
            "NODEPROPERTY",
            "NodEpRoPeRtY"
    })
    void shouldMatchCaseInsensitive(String cypherProvided) {
        var map = CypherMapWrapper.create(Map.of(cypherProvided, 42L));
        assertThat(map)
            .returns(true, cmw -> cmw.containsKey("nodeProperty"))
            .returns(42L, cmw -> cmw.requireLong("nodeProperty"))
            .returns(42L, cmw -> cmw.requireNumber("nodeProperty"))
            .returns(42L, cmw -> cmw.requireChecked("nodeProperty", Long.class));

        assertThatCode(() -> map.requireOnlyKeysFrom(List.of("nodeProperty"))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "nodePropertie",
            "nodepropertie",
            "NodePropertie",
            "NODEPROPERTIE",
            "NodEpRoPeRtIe"
    })
    void shouldSuggestMatchCaseInsensitive(String cypherProvided) {
        var map = CypherMapWrapper.create(Map.of(cypherProvided, 42L));

        assertThatThrownBy(() -> map.requireLong("nodeProperty"))
            .hasMessage("No value specified for the mandatory configuration parameter `nodeProperty` (a similar parameter exists: [%s])", cypherProvided);

        assertThatThrownBy(() -> map.requireOnlyKeysFrom(List.of("nodeProperty")))
            .hasMessage("Unexpected configuration key: %s (Did you mean [nodeProperty]?)", cypherProvided);
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

    static Stream<Arguments> requiredParams() {
        return Stream.of(
            arguments(
                map("foo", 42, "bar", -42),
                "a similar parameter exists: [foo]"
            ),
            arguments(
                map("foo", 42, "bar", -42, "foi", 1337),
                "similar parameters exist: [foi, foo]"
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
            String.format(Locale.ENGLISH, "No value specified for the mandatory configuration parameter `fou` (%s)", expectedMessage),
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
            String.format(Locale.ENGLISH,"Unexpected configuration %s", expectedMessage),
            error.getMessage()
        );
    }

    static Stream<Arguments> mutexParams() {
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
                "Test: No value specified for the mandatory configuration parameter `bbb` (a similar parameter exists: [bb])"
            ),
            arguments(
                map("aaa", 42, "bb", 42, "bbbb", 42),
                "Test: No value specified for the mandatory configuration parameter `bbb` (similar parameters exist: [bbbb, bb])"
            ),
            arguments(
                map("aaa", 42, "bb", 42, "bbbb", 42, "xx", 1337),
                "Test: No value specified for the mandatory configuration parameter `bbb` (similar parameters exist: [bbbb, bb])"
            ),
            arguments(
                map("aaa", 42, "bb", 42, "bbbb", 42, "xxx", 1337),
                "Test: Specify either `aaa` and `bbb` or `xxx` and `yyy`."
            ),
            arguments(
                map("xxx", 42, "yy", 42),
                "Test: No value specified for the mandatory configuration parameter `yyy` (a similar parameter exists: [yy])"
            ),
            arguments(
                map("xxx", 42, "yy", 42, "yyyy", 42),
                "Test: No value specified for the mandatory configuration parameter `yyy` (similar parameters exist: [yyyy, yy])"
            ),
            arguments(
                map("xxx", 42, "yy", 42, "yyyy", 42, "aa", 1337),
                "Test: No value specified for the mandatory configuration parameter `yyy` (similar parameters exist: [yyyy, yy])"
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
    @MethodSource("mutexParams")
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

    private static Map<String, Object> map(Object... objects) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < objects.length; ) {
            map.put((String) objects[i++], objects[i++]);
        }
        return map;
    }
}
