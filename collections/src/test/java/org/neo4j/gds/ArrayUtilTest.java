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
package org.neo4j.gds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.ArrayUtil.oversizeHuge;

class ArrayUtilTest {

    static Stream<Integer> testDataSizes() {
        return Stream.of(32, 33, 64, 65, 2048, 2049, 4096, 4097);
    }

    private int[] setup(int size) {
        int[] testData = new int[size];
        Arrays.setAll(testData, i -> (i + 1) * 2);
        return testData;
    }

    @ParameterizedTest(name = "testDataSize: {0}")
    @MethodSource("testDataSizes")
    void testBinarySearch(int size) {
        int[] testData = setup(size);
        for (int i = 0; i < testData.length; i++) {
            Assertions.assertTrue(
                ArrayUtil.binarySearch(testData, testData.length, (i + 1) * 2),
                String.format(Locale.US, "False negative at %d value %d%n", i, testData[i])
            );
            assertFalse(
                ArrayUtil.binarySearch(testData, testData.length, (i * 2) + 1),
                String.format(Locale.US, "False positive at %d value %d%n", i, testData[i])
            );
        }
    }

    @ParameterizedTest(name = "testDataSize: {0}")
    @MethodSource("testDataSizes")
    void testLinearSearch(int size) {
        int[] testData = setup(size);
        for (int i = 0; i < testData.length; i++) {
            assertTrue(
                ArrayUtil.linearSearch(testData, testData.length, (i + 1) * 2),
                String.format(Locale.US, "False negative at %d value %d%n", i, testData[i])
            );
            assertFalse(
                ArrayUtil.linearSearch(testData, testData.length, (i * 2) + 1),
                String.format(Locale.US, "False positive at %d value %d%n", i, testData[i])
            );
        }
    }

    @Test
    void fillsAnArray() {
        assertArrayEquals(new double[]{1D, 1D, 1D}, ArrayUtil.fill(1D, 3));
        assertArrayEquals(new double[]{}, ArrayUtil.fill(1D, 0));
        assertArrayEquals(new double[]{42D}, ArrayUtil.fill(42D, 1));
    }

    @ParameterizedTest(name = "testDataSize: {0}")
    @MethodSource("testDataSizes")
    void testLinearSearchIndex(int size) {
        int[] testData = setup(size);
        for (int i = 0; i < testData.length; i++) {
            assertThat(ArrayUtil.linearSearchIndex(testData, testData.length, (i + 1) * 2))
                .as(String.format(Locale.US, "False negative at %d value %d%n", i, testData[i]))
                .isEqualTo(i);
            assertThat(ArrayUtil.linearSearchIndex(testData, testData.length, (i * 2) + 1))
                .as(String.format(Locale.US, "False positive at %d value %d%n", i, testData[i]))
                .isEqualTo(-testData.length - 1);
        }
    }

    @Test
    void testBinarySearch() {
        var a = new long[]{0, 2, 2, 2, 2, 2, 2, 5, 6, 6, 6, 6, 7};

        assertThat(Arrays.binarySearch(a, 0, a.length, 0)).isEqualTo(0);
        assertThat(Arrays.binarySearch(a, 0, a.length, 1)).isEqualTo(-2);
        assertThat(Arrays.binarySearch(a, 0, a.length, 2)).isEqualTo(6);
        assertThat(Arrays.binarySearch(a, 0, a.length, 3)).isEqualTo(-8);
        assertThat(Arrays.binarySearch(a, 0, a.length, 4)).isEqualTo(-8);
        assertThat(Arrays.binarySearch(a, 0, a.length, 5)).isEqualTo(7);
        assertThat(Arrays.binarySearch(a, 0, a.length, 6)).isEqualTo(9);
        assertThat(Arrays.binarySearch(a, 0, a.length, 7)).isEqualTo(12);
        assertThat(Arrays.binarySearch(a, 0, a.length, 8)).isEqualTo(-14);
    }

    @Test
    void testBinarySearchFirst() {
        var a = new long[]{0, 2, 2, 2, 2, 2, 2, 5, 6, 6, 6, 6, 7};

        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 0)).isEqualTo(0);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 1)).isEqualTo(-2);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 2)).isEqualTo(1);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 3)).isEqualTo(-8);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 4)).isEqualTo(-8);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 5)).isEqualTo(7);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 6)).isEqualTo(8);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 7)).isEqualTo(12);
        assertThat(ArrayUtil.binarySearchFirst(a, 0, a.length, 8)).isEqualTo(-14);
    }

    @Test
    void testBinarySearchLast() {
        var a = new long[]{0, 2, 2, 2, 2, 2, 2, 5, 6, 6, 6, 6, 7};

        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 0)).isEqualTo(0);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 1)).isEqualTo(-2);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 2)).isEqualTo(6);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 3)).isEqualTo(-8);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 4)).isEqualTo(-8);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 5)).isEqualTo(7);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 6)).isEqualTo(11);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 7)).isEqualTo(12);
        assertThat(ArrayUtil.binarySearchLast(a, 0, a.length, 8)).isEqualTo(-14);
    }


    @Test
    void contains() {
        assertTrue(ArrayUtil.contains(new long[]{1L, -1L, 420L, 0L}, 1L));
        assertTrue(ArrayUtil.contains(new long[]{1L, -1L, 420L, 0L}, -1L));
        assertTrue(ArrayUtil.contains(new long[]{1L, -1L, 420L, 0L}, 420L));
        assertTrue(ArrayUtil.contains(new long[]{1L, -1L, 420L, 0L}, 0L));

        assertFalse(ArrayUtil.contains(new long[]{1L, -1L, 420L, 0L}, 2L));
        assertFalse(ArrayUtil.contains(new long[]{1L, -1L, 420L, 0L}, -420L));

        assertFalse(ArrayUtil.contains(new long[]{}, -420L));
        assertFalse(ArrayUtil.contains(new long[]{}, 0L));
    }

    @Test
    void oversizeIncreasesBy1over8() {
        var actual = oversizeHuge(8 * 42, Integer.BYTES);
        assertEquals(9 * 42, actual);
    }

    @Test
    void oversizeDoesWorkWithHugeArraySizes() {
        var actual = oversizeHuge(1L << 42L, Integer.BYTES);
        assertEquals((1L << 42L) + (1L << 39L), actual);
    }

    @Test
    void oversizeHasMinGrowthForSmallSizes() {
        var actual = oversizeHuge(1, Integer.BYTES);
        assertEquals(4, actual);
    }

    @Test
    void oversizeDoesntGrowForEmptyArrays() {
        var actual = oversizeHuge(0, Integer.BYTES);
        assertEquals(0, actual);
    }

    @Test
    void oversizeAlignsToPointerSize() {
        var actual = oversizeHuge(42, Byte.BYTES);
        // 42 + (42/8) == 47, which gets aligned to 48
        assertEquals(48, actual);
    }
}
