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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TestSupportTest {

    @Test
    void testBinaryCrossArguments() {
        Supplier<Stream<Arguments>> leftFn = () -> Stream.of(Arguments.of(1, 2), Arguments.of(3, 4));
        Supplier<Stream<Arguments>> rightFn = () -> Stream.of(Arguments.of("A", "B"), Arguments.of("C", "D"));
        Stream<Arguments> crossStream = TestSupport.crossArguments(leftFn, rightFn);

        Stream<Arguments> expectedStream = Stream.of(
                Arguments.of(1, 2, "A", "B"),
                Arguments.of(1, 2, "C", "D"),
                Arguments.of(3, 4, "A", "B"),
                Arguments.of(3, 4, "C", "D")
        );

        Assertions.assertEquals(collectArguments(expectedStream), collectArguments(crossStream));
    }

    @Test
    void testNaryCrossArguments() {
        Supplier<Stream<Arguments>> fn1 = () -> Stream.of(Arguments.of(1), Arguments.of(2));
        Supplier<Stream<Arguments>> fn2 = () -> Stream.of(Arguments.of("A"), Arguments.of("B"));
        Supplier<Stream<Arguments>> fn3 = () -> Stream.of(Arguments.of(true), Arguments.of(false));
        Stream<Arguments> crossStream = TestSupport.crossArguments(fn1, fn2, fn3);

        Stream<Arguments> expectedStream = Stream.of(
                Arguments.of(1, "A", true),
                Arguments.of(1, "A", false),
                Arguments.of(1, "B", true),
                Arguments.of(1, "B", false),
                Arguments.of(2, "A", true),
                Arguments.of(2, "A", false),
                Arguments.of(2, "B", true),
                Arguments.of(2, "B", false)
        );

        Assertions.assertEquals(collectArguments(expectedStream), collectArguments(crossStream));
    }

    private Set<List<Object>> collectArguments(Stream<Arguments> arguments) {
        return arguments.map(Arguments::get).map(Arrays::asList).collect(Collectors.toSet());
    }

}