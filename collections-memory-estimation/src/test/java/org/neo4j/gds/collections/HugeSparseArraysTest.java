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
import static org.neo4j.gds.collections.HugeSparseArrays.DEFAULT_PAGE_SHIFT;

class HugeSparseArraysTest {

    static Stream<Class<?>> valueClasses() {
        return Stream.of(
            byte.class,
            short.class,
            int.class,
            long.class,
            float.class,
            double.class,
            byte[].class,
            short[].class,
            int[].class,
            long[].class,
            float[].class,
            double[].class
        );
    }

    @ParameterizedTest
    @MethodSource("valueClasses")
    void shouldComputeMemoryEstimationForBestCase(Class<?> valueClazz) {
        long size = integer(Integer.MAX_VALUE);

        var memoryRange = valueClazz.isPrimitive()
            ? HugeSparseArrays.estimatePrimitive(valueClazz, size, size, DEFAULT_PAGE_SHIFT)
            : HugeSparseArrays.estimateArray(valueClazz, size, size, 0, DEFAULT_PAGE_SHIFT);

        assertThat(memoryRange.min).isEqualTo(memoryRange.max);
    }

    @ParameterizedTest
    @MethodSource("valueClasses")
    void shouldComputeMemoryEstimationForWorstCase(Class<?> valueClazz) {
        long pageSize = 1 << DEFAULT_PAGE_SHIFT;
        long maxEntries = integer(Integer.MAX_VALUE);
        long maxId = between(maxEntries + pageSize, maxEntries * pageSize).Long();

        var memoryRange = valueClazz.isPrimitive()
            ? HugeSparseArrays.estimatePrimitive(valueClazz, maxId, maxEntries, DEFAULT_PAGE_SHIFT)
            : HugeSparseArrays.estimateArray(valueClazz, maxId, maxEntries, 0, DEFAULT_PAGE_SHIFT);

        assertThat(memoryRange.min).isLessThan(memoryRange.max);
    }

    static Stream<Arguments> expectedRangesPrimitiveTypes() {
        return Stream.of(
            // byte
            Arguments.of(byte.class, 0L, 0L, 48L, 48L),
            Arguments.of(byte.class, 100L, 100L, 4_168L, 4_168L),
            Arguments.of(byte.class, 100_000_000_000L, 1L, 97_660_416L, 97_660_416L),
            Arguments.of(byte.class, 100_000_000_000L, 10_000_000L, 107_697_808L, 41_217_656_304L),
            Arguments.of(byte.class, 100_000_000_000L, 100_000_000L, 198_050_784L, 100_488_283_360L),
            // short
            Arguments.of(short.class, 0L, 0L, 48L, 48L),
            Arguments.of(short.class, 100L, 100L, 8_264L, 8_264L),
            Arguments.of(short.class, 100_000_000_000L, 1L, 97_664_512L, 97_664_512L),
            Arguments.of(short.class, 100_000_000_000L, 10_000_000L, 117_700_240L, 82_177_656_304L),
            Arguments.of(short.class, 100_000_000_000L, 100_000_000L, 298_054_624L, 200_488_285_408L),
            // int
            Arguments.of(int.class, 0L, 0L, 48L, 48L),
            Arguments.of(int.class, 100L, 100L, 16456L, 16456L),
            Arguments.of(int.class, 100_000_000_000L, 1L, 97_672_704L, 97_672_704L),
            Arguments.of(int.class, 100_000_000_000L, 10_000_000L, 137_705_104L, 164_097_656_304L),
            Arguments.of(int.class, 100_000_000_000L, 100_000_000L, 498_062_304L, 400_488_289_504L),
            // long
            Arguments.of(long.class, 0L, 0L, 48L, 48L),
            Arguments.of(long.class, 100L, 100L, 32840L, 32840L),
            Arguments.of(long.class, 100_000_000_000L, 1L, 97_689_088L, 97_689_088L),
            Arguments.of(long.class, 100_000_000_000L, 10_000_000L, 177_714_832L, 327_937_656_304L),
            Arguments.of(long.class, 100_000_000_000L, 100_000_000L, 898_077_664L, 800_488_297_696L),
            // float
            Arguments.of(float.class, 0L, 0L, 48L, 48L),
            Arguments.of(float.class, 100L, 100L, 16456L, 16456L),
            Arguments.of(float.class, 100_000_000_000L, 1L, 97_672_704L, 97_672_704L),
            Arguments.of(float.class, 100_000_000_000L, 10_000_000L, 137_705_104L, 164_097_656_304L),
            Arguments.of(float.class, 100_000_000_000L, 100_000_000L, 498_062_304L, 400_488_289_504L),
            // double
            Arguments.of(double.class, 0L, 0L, 48L, 48L),
            Arguments.of(double.class, 100L, 100L, 32840L, 32840L),
            Arguments.of(double.class, 100_000_000_000L, 1L, 97_689_088L, 97_689_088L),
            Arguments.of(double.class, 100_000_000_000L, 10_000_000L, 177_714_832L, 327_937_656_304L),
            Arguments.of(double.class, 100_000_000_000L, 100_000_000L, 898_077_664L, 800_488_297_696L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedRangesPrimitiveTypes")
    void shouldComputeMemoryEstimation(
        Class<?> valueClazz,
        long maxId,
        long maxEntries,
        long expectedMin,
        long expectedMax
    ) {
        assertThat(MemoryRange.of(expectedMin, expectedMax))
            .isEqualTo(HugeSparseArrays.estimatePrimitive(
                valueClazz,
                maxId,
                maxEntries,
                DEFAULT_PAGE_SHIFT
            ));
    }

    static Stream<Arguments> expectedRangesArrayTypes() {
        return Stream.of(
            // byte
            Arguments.of(byte[].class, 0, 0L, 0L, 48L, 48L),
            Arguments.of(byte[].class, 10, 0L, 0L, 48L, 48L),
            Arguments.of(byte[].class, 0, 100L, 100L, 18_056L, 18_056L),
            Arguments.of(byte[].class, 10, 100L, 100L, 19_656L, 19_656L),
            Arguments.of(byte[].class, 0, 100_000_000_000L, 1L, 97_672_720L, 97_672_720L),
            Arguments.of(byte[].class, 10, 100_000_000_000L, 1L, 97_672_736L, 97_672_736L),
            Arguments.of(byte[].class, 0, 100_000_000_000L, 10_000_000L, 297_705_104L, 164_257_656_304L),
            Arguments.of(byte[].class, 10, 100_000_000_000L, 10_000_000L, 457_705_104L, 164_417_656_304L),
            Arguments.of(byte[].class, 0, 100_000_000_000L, 100_000_000L, 2_098_062_304L, 402_088_289_504L),
            Arguments.of(byte[].class, 10, 100_000_000_000L, 100_000_000L, 3_698_062_304L, 403_688_289_504L),
            // short
            Arguments.of(short[].class, 0, 0L, 0L, 48L, 48L),
            Arguments.of(short[].class, 10, 0L, 0L, 48L, 48L),
            Arguments.of(short[].class, 0, 100L, 100L, 18056L, 18056L),
            Arguments.of(short[].class, 10, 100L, 100L, 20456L, 20456L),
            Arguments.of(short[].class, 0, 100_000_000_000L, 1L, 97672720L, 97672720L),
            Arguments.of(short[].class, 10, 100_000_000_000L, 1L, 97672744L, 97672744L),
            Arguments.of(short[].class, 0, 100_000_000_000L, 10_000_000L, 297705104L, 164257656304L),
            Arguments.of(short[].class, 10, 100_000_000_000L, 10_000_000L, 537705104L, 164497656304L),
            Arguments.of(short[].class, 0, 100_000_000_000L, 100_000_000L, 2098062304L, 402088289504L),
            Arguments.of(short[].class, 10, 100_000_000_000L, 100_000_000L, 4498062304L, 404488289504L),
            // int
            Arguments.of(int[].class, 0, 0L, 0L, 48L, 48L),
            Arguments.of(int[].class, 10, 0L, 0L, 48L, 48L),
            Arguments.of(int[].class, 0, 100L, 100L, 18056L, 18056L),
            Arguments.of(int[].class, 10, 100L, 100L, 22056L, 22056L),
            Arguments.of(int[].class, 0, 100_000_000_000L, 1L, 97672720L, 97672720L),
            Arguments.of(int[].class, 10, 100_000_000_000L, 1L, 97672760L, 97672760L),
            Arguments.of(int[].class, 0, 100_000_000_000L, 10_000_000L, 297705104L, 164257656304L),
            Arguments.of(int[].class, 10, 100_000_000_000L, 10_000_000L, 697705104L, 164657656304L),
            Arguments.of(int[].class, 0, 100_000_000_000L, 100_000_000L, 2098062304L, 402088289504L),
            Arguments.of(int[].class, 10, 100_000_000_000L, 100_000_000L, 6098062304L, 406088289504L),
            // long
            Arguments.of(long[].class, 0, 0L, 0L, 48L, 48L),
            Arguments.of(long[].class, 10, 0L, 0L, 48L, 48L),
            Arguments.of(long[].class, 0, 100L, 100L, 18056L, 18056L),
            Arguments.of(long[].class, 10, 100L, 100L, 26056L, 26056L),
            Arguments.of(long[].class, 0, 100_000_000_000L, 1L, 97672720L, 97672720L),
            Arguments.of(long[].class, 10, 100_000_000_000L, 1L, 97672800L, 97672800L),
            Arguments.of(long[].class, 0, 100_000_000_000L, 10_000_000L, 297705104L, 164257656304L),
            Arguments.of(long[].class, 10, 100_000_000_000L, 10_000_000L, 1097705104L, 165057656304L),
            Arguments.of(long[].class, 0, 100_000_000_000L, 100_000_000L, 2098062304L, 402088289504L),
            Arguments.of(long[].class, 10, 100_000_000_000L, 100_000_000L, 10098062304L, 410088289504L),
            // float
            Arguments.of(float[].class, 0, 0L, 0L, 48L, 48L),
            Arguments.of(float[].class, 10, 0L, 0L, 48L, 48L),
            Arguments.of(float[].class, 0, 100L, 100L, 18056L, 18056L),
            Arguments.of(float[].class, 10, 100L, 100L, 22056L, 22056L),
            Arguments.of(float[].class, 0, 100_000_000_000L, 1L, 97672720L, 97672720L),
            Arguments.of(float[].class, 10, 100_000_000_000L, 1L, 97672760L, 97672760L),
            Arguments.of(float[].class, 0, 100_000_000_000L, 10_000_000L, 297705104L, 164257656304L),
            Arguments.of(float[].class, 10, 100_000_000_000L, 10_000_000L, 697705104L, 164657656304L),
            Arguments.of(float[].class, 0, 100_000_000_000L, 100_000_000L, 2098062304L, 402088289504L),
            Arguments.of(float[].class, 10, 100_000_000_000L, 100_000_000L, 6098062304L, 406088289504L),
            // double
            Arguments.of(double[].class, 0, 0L, 0L, 48L, 48L),
            Arguments.of(double[].class, 10, 0L, 0L, 48L, 48L),
            Arguments.of(double[].class, 0, 100L, 100L, 18056L, 18056L),
            Arguments.of(double[].class, 10, 100L, 100L, 26056L, 26056L),
            Arguments.of(double[].class, 0, 100_000_000_000L, 1L, 97672720L, 97672720L),
            Arguments.of(double[].class, 10, 100_000_000_000L, 1L, 97672800L, 97672800L),
            Arguments.of(double[].class, 0, 100_000_000_000L, 10_000_000L, 297705104L, 164257656304L),
            Arguments.of(double[].class, 10, 100_000_000_000L, 10_000_000L, 1097705104L, 165057656304L),
            Arguments.of(double[].class, 0, 100_000_000_000L, 100_000_000L, 2098062304L, 402088289504L),
            Arguments.of(double[].class, 10, 100_000_000_000L, 100_000_000L, 10098062304L, 410088289504L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedRangesArrayTypes")
    void shouldComputeMemoryEstimationForArrayTypes(
        Class<?> valueClazz,
        int averageEntryLength,
        long maxId,
        long maxEntries,
        long expectedMin,
        long expectedMax
    ) {
        assertThat(MemoryRange.of(expectedMin, expectedMax)).isEqualTo(HugeSparseArrays.estimateArray(
            valueClazz,
            maxId,
            maxEntries,
            averageEntryLength,
            DEFAULT_PAGE_SHIFT
        ));
    }


}
