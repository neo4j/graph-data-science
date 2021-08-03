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

import org.eclipse.collections.api.tuple.primitive.IntIntPair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.utils.ArrayLayout;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples.pair;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ArrayLayoutTest {

    @ParameterizedTest
    @MethodSource("layouts")
    void testConstruct(long[] input, long[] expected) {
        var result = ArrayLayout.constructEytzinger(input);
        assertThat(result)
            .contains(-1L, atIndex(0))
            .containsSubsequence(expected);
    }

    static Stream<Arguments> layouts() {
        return Stream.of(
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8},
                new long[]{5, 3, 7, 2, 4, 6, 8, 1}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9},
                new long[]{6, 4, 8, 2, 5, 7, 9, 1, 3}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
                new long[]{7, 4, 9, 2, 6, 8, 10, 1, 3, 5}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                new long[]{8, 4, 10, 2, 6, 9, 11, 1, 3, 5, 7}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
                new long[]{8, 4, 11, 2, 6, 10, 12, 1, 3, 5, 7, 9}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
                new long[]{8, 4, 12, 2, 6, 10, 13, 1, 3, 5, 7, 9, 11}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14},
                new long[]{8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13}
            ),
            arguments(
                new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                new long[]{8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13, 15}
            )
        );
    }

    @Test
    void testSearch() {
        var input = new long[]{1, 2, 3, 4, 5, 6, 7, 8};
        var layout = ArrayLayout.constructEytzinger(input);
        assertThat(ArrayLayout.searchEytzinger(layout, 1)).isEqualTo(7 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 2)).isEqualTo(3 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 3)).isEqualTo(1 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 4)).isEqualTo(4 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 5)).isEqualTo(0 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 6)).isEqualTo(5 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 7)).isEqualTo(2 + 1);
        assertThat(ArrayLayout.searchEytzinger(layout, 8)).isEqualTo(6 + 1);
    }

    @Test
    void testMissingValue() {
        var input = new long[]{1, 2, 3, 4, 5, 6, 7, 8};
        var layout = ArrayLayout.constructEytzinger(input);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 0)]).isEqualTo(-1);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 9)]).isEqualTo(8);
    }

    @ParameterizedTest
    @MethodSource("duplicateValues")
    void testDuplicateValues(long[] input) {
        var layout = ArrayLayout.constructEytzinger(input);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 1)]).isEqualTo(1L);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 2)]).isEqualTo(1L);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 3)]).isEqualTo(3L);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 4)]).isEqualTo(3L);
        assertThat(layout[ArrayLayout.searchEytzinger(layout, 7)]).isEqualTo(7L);
    }

    static Stream<long[]> duplicateValues() {
        return Stream.of(
            new long[]{1, 3, 3, 7},
            new long[]{1, 3, 3, 7, 7},
            new long[]{1, 1, 3, 3, 7, 7},
            new long[]{1, 1, 3, 3, 3, 7, 7},
            new long[]{1, 1, 3, 3, 3, 3, 7, 7}
        );
    }

    @Test
    void testSearchFib() {
        var values = Stream.iterate(pair(0, 1), pair -> pair(pair.getTwo(), pair.getOne() + pair.getTwo()))
            .skip(2)
            .limit(15)
            .mapToLong(IntIntPair::getOne)
            .toArray();
        var layout = ArrayLayout.constructEytzinger(values);
        for (int i = 0; i < 1000; i++) {
            var searchEytzinger = ArrayLayout.searchEytzinger(layout, i);
            var eytzingerValue = layout[searchEytzinger];

            var binarySearch = Arrays.binarySearch(values, i);
            // binarySearch returns (-idx - 1) when not found, and the index is that of the next element that
            // is larger, whereas we return the one that is smaller, so we do -2 to get to the correct index
            binarySearch = binarySearch >= 0 ? binarySearch : -binarySearch - 2;
            var binarySearchValue = binarySearch >= 0 ? values[binarySearch] : -1;
            assertThat(eytzingerValue)
                .as(
                    "Needle = %d Eytzinger result = %d, Binary Search result = %d",
                    i,
                    eytzingerValue,
                    binarySearchValue
                )
                .isEqualTo(binarySearchValue);
        }
    }

    @Test
    void testSecondaryIndex() {
        long[] values = {13, 37, 42};
        int[] secondaryValues = {1, 2, 3};

        var layouts = ArrayLayout.constructEytzinger(values, secondaryValues);
        assertThat(layouts.secondary()).containsExactly(2, 1, 3);
    }
}
