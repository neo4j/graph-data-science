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
package org.neo4j.graphalgo.core.heavyweight;

import com.carrotsearch.hppc.sorting.IndirectComparator.AscendingIntComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public final class IndirectIntSortTest extends RandomizedTest {

    @Test
    public void sortsBasedOnFirstArray() {
        int size = iterations(10, 100);
        int[] values = randomArray(size);
        // use the index as sidecar data to make it easier to compare the sort results
        float[] data = indexArray(size);

        // sort result should be the same as whatever IndirectSort produces
        int[] expected = IndirectSort.mergesort(0, size, new AscendingIntComparator(values));
        IndirectIntSort.sortWithoutDeduplication(values, data, new long[size], size);

        assertSameIndices(expected, data);
    }

    public void onlySortsTheSmallestAvailableSegment() {
        int size = iterations(20, 200);
        // smaller data and value
        int valuesSize = between(1, size - 1);
        int[] values = randomArray(valuesSize);
        int dataSize = between(1, size - 1);
        float[] data = indexArray(dataSize);

        int smallestSize = Math.min(valuesSize, dataSize);
        int[] expected = IndirectSort.mergesort(0, smallestSize, new AscendingIntComparator(values));
        if (dataSize > valuesSize) {
            expected = Arrays.copyOf(expected, dataSize);
            System.arraycopy(data, smallestSize, expected, smallestSize, dataSize - smallestSize);
        }

        IndirectIntSort.sortWithoutDeduplication(values, data, new long[size], size);
        assertSameIndices(expected, data);
    }

    @Test
    public void removeDuplicates() {
        int size = iterations(10, 100);
        int[] values = randomArray(size);

        int[] expected = IndirectSort.mergesort(0, size, new AscendingIntComparator(values));

        // double the contents before our search
        // with deduplication, the result should be the same as without this doubling
        int doubleSize = size * 2;
        values = Arrays.copyOf(values, doubleSize);
        System.arraycopy(values, 0, values, size, size);
        float[] data = indexArray(doubleSize);
        int newSize = IndirectIntSort.sort(values, data, new long[doubleSize], doubleSize);

        assertEquals(size, newSize);
        assertSameIndices(expected, Arrays.copyOf(data, newSize));
    }

    @Test
    public void hasInternalBuffer() {
        int size = iterations(10, 100);
        int[] values = randomArray(size);
        float[] data = indexArray(size);

        int[] expected = IndirectSort.mergesort(0, size, new AscendingIntComparator(values));
        new IndirectIntSort().sortWithoutDeduplication(values, data, size);

        assertSameIndices(expected, data);
    }

    @Test
    public void internalBufferResizes() {
        int size = iterations(10, 100);
        int[] values = randomArray(size);
        float[] data = indexArray(size);
        int[] expected = IndirectSort.mergesort(0, size, new AscendingIntComparator(values));

        IndirectIntSort indirectIntSort = new IndirectIntSort();
        indirectIntSort.sortWithoutDeduplication(values, data, 1); // "sort" first value, buffer should be size 1
        indirectIntSort.sortWithoutDeduplication(values, data, size); // "sort" all values now, buffer should be resized

        assertSameIndices(expected, data);
    }

    @Test(expected = AssertionError.class)
    public void failsIfBufferIsTooSmall() {
        IndirectIntSort.sortWithoutDeduplication(new int[10], new float[10], new long[0], 10);
    }

    @Test(expected = AssertionError.class)
    public void failsIfBufferIsTooSmall2() {
        IndirectIntSort.sort(new int[10], new float[10], new long[0], 10);
    }

    private static int[] randomArray(int size) {
        return IntStream.generate(() -> RandomizedTest.randomInt(Integer.MAX_VALUE)).distinct().limit(size).toArray();
    }

    private static float[] indexArray(int size) {
        float[] data = new float[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (float) i;
        }
        return data;
    }

    private static void assertSameIndices(int[] expected, float[] actual) {
        int[] actualAsInt = new int[actual.length];
        Arrays.setAll(actualAsInt, i -> (int) actual[i]);
        assertArrayEquals(expected, actualAsInt);
    }
}
