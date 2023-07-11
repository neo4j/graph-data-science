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
package org.neo4j.gds.core.compression;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedHistogramTest {

    static Stream<Arguments> means() {
        return Stream.of(
            Arguments.of(new int[] {5}, 5),
            Arguments.of(IntStream.range(0, 10).toArray(), 4.5),
            Arguments.of(IntStream.range(1, 10).toArray(), 5),
            Arguments.of(new int[] {1, 4, 4, 4}, 3.25)
        );
    }

    @ParameterizedTest
    @MethodSource("means")
    void testMean(int[] values, double expected) {
        var histogram = create(values);
        assertThat(histogram.mean()).isEqualTo(expected);
    }

    static Stream<Arguments> medians() {
        return Stream.of(
            Arguments.of(new int[] {5}, 5),
            Arguments.of(IntStream.range(0, 10).toArray(), 5),
            Arguments.of(IntStream.range(1, 10).toArray(), 6),
            Arguments.of(new int[] {1, 4, 4, 4}, 4)
        );
    }

    @ParameterizedTest
    @MethodSource("medians")
    void testMedian(int[] values, int expected) {
        var histogram = create(values);
        assertThat(histogram.median()).isEqualTo(expected);
    }

    static Stream<Arguments> mins() {
        return Stream.of(
            Arguments.of(new int[] {}, BoundedHistogram.NO_VALUE),
            Arguments.of(new int[] {5}, 5),
            Arguments.of(IntStream.range(0, 10).toArray(), 0),
            Arguments.of(IntStream.range(1, 10).toArray(), 1),
            Arguments.of(new int[] {1, 4, 4, 4}, 1)
        );
    }

    @ParameterizedTest
    @MethodSource("mins")
    void testMin(int[] values, int expected) {
        var histogram = create(values);
        assertThat(histogram.min()).isEqualTo(expected);
    }

    static Stream<Arguments> maxs() {
        return Stream.of(
            Arguments.of(new int[] {}, BoundedHistogram.NO_VALUE),
            Arguments.of(new int[] {5}, 5),
            Arguments.of(IntStream.range(0, 10).toArray(), 9),
            Arguments.of(IntStream.range(1, 10).toArray(), 9),
            Arguments.of(new int[] {1, 4, 4, 4}, 4),
            Arguments.of(new int[] {1, 1, 2, 4}, 4),
            Arguments.of(new int[] {1, 1, 2, 2, 4, 4, 4}, 4)
        );
    }

    @ParameterizedTest
    @MethodSource("maxs")
    void testMax(int[] values, int expected) {
        var histogram = create(values);
        assertThat(histogram.max()).isEqualTo(expected);
    }

    static Stream<Arguments> adds() {
        return Stream.of(
            Arguments.of(new int[] {}, new int[] {42}, 1),
            Arguments.of(new int[] {5}, new int[] {5}, 2),
            Arguments.of(IntStream.range(0, 10).toArray(), new int[] {42}, 11),
            Arguments.of(new int[] {42}, IntStream.range(0, 10).toArray(), 11)
        );
    }

    @ParameterizedTest
    @MethodSource("adds")
    void testAdd(int[] first, int[] second, int expected) {
        var histogram = create(first);
        histogram.add(create(second));
        assertThat(histogram.total()).isEqualTo(expected);
    }

    static BoundedHistogram create(int... values) {
        int maxValue = Arrays.stream(values).max().orElse(0);
        var histogram = new BoundedHistogram(maxValue + 1);
        Arrays.stream(values).forEach(histogram::record);
        return histogram;
    }
}
