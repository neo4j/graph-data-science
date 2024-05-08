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
package org.neo4j.gds.collections.ha;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.mem.Estimate;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;

final class HugeObjectArrayTest extends HugeArrayTestBase<String[], String, HugeObjectArray<String>> {

    private static final int NODE_COUNT = 42;

    @Test
    void shouldCreateEmptyArray() {
        var array = HugeObjectArray.of();
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(0L);
    }

    @ParameterizedTest
    @ValueSource(longs = {42, 268435498})
    void shouldComputeMemoryEstimation(long elementCount) {
        var elementEstimation = sizeOfLongArray(42);
        var lowerBoundEstimate = elementCount * elementEstimation;

        var estimation = HugeObjectArray.memoryEstimation(elementCount, elementEstimation);
        assertThat(estimation).isCloseTo(lowerBoundEstimate, Percentage.withPercentage(2));
    }

    @ParameterizedTest
    @MethodSource("longsArrays")
    void shouldReturnDefaultValueIfNull(HugeObjectArray<long[]> longArrays) {
        long[] fillValue = new long[] { 42 };
        long[] defaultValue = new long[] { 1337 };
        long updateIndex = NODE_COUNT / 2;

        longArrays.fill(fillValue);

        assertThat(longArrays.get(updateIndex)).isEqualTo(fillValue);
        longArrays.set(updateIndex, null);
        assertThat(longArrays.get(updateIndex)).isNull();
        assertThat(longArrays.getOrDefault(updateIndex, defaultValue)).isEqualTo(defaultValue);
    }

    static Stream<HugeObjectArray<long[]>> longsArrays() {
        return Stream.of(
            HugeObjectArray.newSingleArray(long[].class, NODE_COUNT),
            HugeObjectArray.newPagedArray(long[].class, NODE_COUNT)
        );
    }

    @Override
    HugeObjectArray<String> singleArray(final int size) {
        return HugeObjectArray.newSingleArray(String.class, size);
    }

    @Override
    HugeObjectArray<String> pagedArray(final int size) {
        return HugeObjectArray.newPagedArray(String.class, size);
    }

    @Override
    long bufferSize(final int size) {
        return Estimate.sizeOfObjectArray(size);
    }

    @Override
    String box(final int value) {
        return value + "";
    }

    @Override
    int unbox(final String value) {
        return value == null ? 0 : Integer.parseInt(value);
    }

    @Override
    String primitiveNull() {
        return null;
    }
}
