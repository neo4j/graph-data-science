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
package org.neo4j.gds.core.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class IntersectionsTest {

    static Stream<Arguments> testData() {
        return Stream.of(
                arguments(new long[]{-3,-2,-1,11,12,13,14,15}, Optional.empty(), new long[]{0,1,2,3,4,5,6}),
                arguments(new long[]{0}, Optional.empty(), new long[]{1,2,3,4,5,6}),
                arguments(new long[]{1,3,6,7}, Optional.empty(), new long[]{0,2,4,5}),
                arguments(new long[]{1,3,6,7}, Optional.of(1), new long[]{0,2,3,4,5,6}),
                arguments(new long[]{0,1,2,3,4,5,6,9}, Optional.empty(), new long[0])
            );
    }

    @ParameterizedTest
    @MethodSource("testData")
    void shouldComputeDifferenceCorrectly(long[] exclude,  Optional<Integer> bound, long[] expected){
        var include = new long[]{0,1,2,3,4,5,6};

        assertThat(Intersections.sortedDifference(include,exclude, bound)).containsExactly(expected);

    }

}
