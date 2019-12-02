/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.core.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ProjectionParserTest {

    @ParameterizedTest
    @MethodSource("validInput")
    void testValidInput(String input, Collection<String> expected) {
        Set<String> result = ProjectionParser.parse(input);
        String[] actual = result.toArray(new String[0]);
        assertArrayEquals(expected.toArray(new String[0]), actual);
    }

    @ParameterizedTest
    @MethodSource("invalidInput")
    void testInValidInput(String input) {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ProjectionParser.parse(input));
        assertTrue(error.getMessage().startsWith("Could not parse projection: expected letter, digit, or underscore"));
    }

    static Stream<Arguments> validInput() {
        return Stream.of(
                arguments(null, emptySet()),
                arguments("", emptySet()),
                arguments("_", singleton("_")),
                arguments("SIMPLE", singleton("SIMPLE")),
                arguments(":SIMPLE", singleton("SIMPLE")),
                arguments("`SIMPLE`", singleton("SIMPLE")),
                arguments("`:SIMPLE`", singleton(":SIMPLE")),
                arguments("SIMPLE_TYPE", singleton("SIMPLE_TYPE")),
                arguments("`TYPE WITH SPACES`", singleton("TYPE WITH SPACES")),
                arguments(":`TYPE WITH SPACES`", singleton("TYPE WITH SPACES")),
                arguments("`:`", singleton(":")),
                arguments(":`💩`", singleton("💩")),
                arguments("TYPE1|TYPE2", asList("TYPE1", "TYPE2")),
                arguments(":TYPE1|TYPE2", asList("TYPE1", "TYPE2")),
                arguments("TYPE1|:TYPE2", asList("TYPE1", "TYPE2")),
                arguments(":TYPE1|:TYPE2", asList("TYPE1", "TYPE2")),
                arguments("`TYPE1|TYPE2`", singleton("TYPE1|TYPE2")),
                arguments("`TYPE1`|`TYPE2`", asList("TYPE1", "TYPE2")),
                arguments("`:TYPE1`|`:TYPE2`", asList(":TYPE1", ":TYPE2"))
        );
    }

    static Stream<String> invalidInput() {
        return Stream.of(
                "-",
                "FOO-BAR",
                "::FOO",
                "FOO:",
                "`FOO",
                "FOO`",
                "``",
                "|",
                "FOO|",
                "|FOO"
        );
    }
}
