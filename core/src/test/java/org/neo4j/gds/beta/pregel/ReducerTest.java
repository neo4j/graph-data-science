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
package org.neo4j.gds.beta.pregel;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.beta.pregel.Reducer;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReducerTest {

    static Stream<Arguments> arguments() {
        return Stream.of(
            Arguments.of(new Reducer.Sum(), 2, 2, 4),
            Arguments.of(new Reducer.Min(), 42, 23, 23),
            Arguments.of(new Reducer.Max(), 42, 23, 42),
            Arguments.of(new Reducer.Count(), 42, 23, 43)
        );
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void sum(Reducer reducer, double arg0, double arg1, double expected) {
        assertEquals(expected, reducer.reduce(arg0, arg1));
    }

}
