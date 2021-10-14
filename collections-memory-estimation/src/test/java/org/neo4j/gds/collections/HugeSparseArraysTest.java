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
package org.neo4j.gds.collections;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.stream.Stream;

import static io.qala.datagen.RandomShortApi.integer;
import static io.qala.datagen.RandomValue.between;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.collections.HugeSparseArrays.DEFAULT_PAGE_SHIFT;

class HugeSparseArraysTest {

    static Stream<Class<?>> valueClasses() {
        return Stream.of(
            long.class,
            long[].class
        );
    }

    @ParameterizedTest
    @MethodSource("valueClasses")
    void shouldComputeMemoryEstimationForBestCase(Class<?> valueClazz) {
        long size = integer(Integer.MAX_VALUE);

        var memoryRange = HugeSparseArrays.estimate(
            valueClazz,
            size,
            size,
            DEFAULT_PAGE_SHIFT
        );

        assertThat(memoryRange.min).isEqualTo(memoryRange.max);
    }

    @ParameterizedTest
    @MethodSource("valueClasses")
    void shouldComputeMemoryEstimationForWorstCase(Class<?> valueClazz) {
        long pageSize = 1 << DEFAULT_PAGE_SHIFT;
        long maxEntries = integer(Integer.MAX_VALUE);
        long maxId = between(maxEntries + pageSize, maxEntries * pageSize).Long();

        var memoryRange = HugeSparseArrays.estimate(
            valueClazz,
            maxId,
            maxEntries,
            DEFAULT_PAGE_SHIFT
        );

        assertThat(memoryRange.min).isLessThan(memoryRange.max);
    }

    static Stream<Arguments> expectedRanges() {
        return Stream.of(
            Arguments.of(long.class, 0L, 0L, 48L, 48L),
            Arguments.of(long.class, 100L, 100L, 32840L, 32840L),
            Arguments.of(long.class, 100_000_000_000L, 1L, 97_689_088L, 97_689_088L),
            Arguments.of(long.class, 100_000_000_000L, 10_000_000L, 177_714_832L, 327_937_656_304L),
            Arguments.of(long.class, 100_000_000_000L, 100_000_000L, 898_077_664L, 800_488_297_696L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedRanges")
    void shouldComputeMemoryEstimation(
        Class<?> valueClazz,
        long maxId,
        long maxEntries,
        long expectedMin,
        long expectedMax
    ) {
        assertEquals(
            MemoryRange.of(expectedMin, expectedMax),
            HugeSparseArrays.estimate(valueClazz, maxId, maxEntries, DEFAULT_PAGE_SHIFT)
        );
    }
}
