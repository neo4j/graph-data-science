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

import com.carrotsearch.hppc.sorting.IndirectComparator;

import java.util.Arrays;

/**
 * Helper class to sort parallel arrays of int-float by the values of the int array while moving the
 * values of the float accordingly (sometimes referred to as indirect sort).
 * <b>only supports positive values</b>
 *
 * The parallel arrays are usually values from a struct-of-arrays encoding of two values per entity.
 * One of those values is used as the ordering key (called "values") and the values from the other
 * array (called "sidecar") are moved based on the new order of the first values array.
 *
 * The implementation packs the two values into a long value with the order relevant value
 * occupying the more significant bits. The bits of the float value will be interpreted as int
 * without casting the float by value. We then call {@link Arrays#sort(long[])} on the buffer,
 * which will sort in first order by the more significant bits – the values – and the sidecar data
 * is "dragged along" with the value data. Afterwards, the values are written into the provided
 * arrays, overwriting their previous contents.
 *
 * The JDK does not offer an equivalent API, as it is biased towards an array-of-structs approach.
 * In order to sort the same data with default JDK APIs, we would have to create a new class for
 * the two values (or reuse a generic one like Entry, Pair, or Tuple), put those into a list and sort it.
 * This approach uses too much object allocation (and possibly Integer/Float boxing as well).
 *
 * HPPC offers an API for indirect sorting under {@link com.carrotsearch.hppc.sorting.IndirectSort},
 * which uses a more generic approach for indirect sorting. Instead of packing the values into a single long,
 * HPPC creates an additional int[] array for the indexes (actually two index arrays, due to its merge sort implementation),
 * sorts this index array by using a Comparator the does an indirect array lookup into the the values array and
 * return the sorted index array, requiring us to reorder both arrays by writing into a fresh copy.
 * This approach requires to many copies of either the data itself or the index values.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Parallel_array">Wikipedia: Parallel array</a>
 * @see com.carrotsearch.hppc.sorting.IndirectSort#mergesort(int, int, IndirectComparator)
 */
final class IndirectIntSort {

    private static final long[] EMPTY_LONGS = new long[0];

    private long[] combinedBuffer = EMPTY_LONGS;

    /**
     * Sort the {@code values} array (upto, but not including, the index {@code length})
     * and reorde the entries in {@code sidecar} accordingly. The values are deduplicated
     * at the end.
     *
     * @return the new length of valid data in the values array. This is either the same value
     *         as the provided length or less, due to deduplication. The arrays are not trunctated
     *         and may contain garbage data in index positions that are after the returned length.
     */
    int sort(int[] values, float[] sidecar, int length) {
        length = Math.min(length, Math.min(values.length, sidecar.length));
        if (length > combinedBuffer.length) {
            combinedBuffer = new long[length];
        }
        return sort(values, sidecar, combinedBuffer, length);
    }

    /**
     * Sort the {@code values} array (upto, but not including, the index {@code length})
     * and reorde the entries in {@code sidecar} accordingly.
     * Unlike {@link #sort(int[], float[], int)}, no deduplication happens and the returned value
     * would always be the same as the provided length, so no value is returned instead.
     */
    void sortWithoutDeduplication(int[] values, float[] sidecar, int length) {
        length = Math.min(length, Math.min(values.length, sidecar.length));
        if (length > combinedBuffer.length) {
            combinedBuffer = new long[length];
        }
        sortWithoutDeduplication(values, sidecar, combinedBuffer, length);
    }

    /**
     * Static version of {@link #sort(int[], float[], int)} that uses an external buffer.
     * The user has to make sure that the buffer is approriately sized.
     *
     * @see #sort(int[], float[], int)
     */
    static int sort(int[] values, float[] sidecar, long[] buffer, int length) {
        justSort(values, sidecar, buffer, length);

        int write = 0;
        for (int i = 0, prev = -1; i < length; i++) {
            long combinedValue = buffer[i];
            int value = (int) (combinedValue >> 32);
            if (value > prev) {
                sidecar[write] = Float.intBitsToFloat((int) combinedValue);
                values[write++] = value;
                prev = value;
            }
        }
        return write;
    }

    /**
     * Static version of {@link #sortWithoutDeduplication(int[], float[], int)} that uses an external buffer.
     * The user has to make sure that the buffer is approriately sized.
     *
     * @see #sortWithoutDeduplication(int[], float[], int)
     */
    static void sortWithoutDeduplication(int[] values, float[] sidecar, long[] buffer, int length) {
        justSort(values, sidecar, buffer, length);

        for (int index = 0; index < length; ++index) {
            long combinedValue = buffer[index];
            sidecar[index] = Float.intBitsToFloat((int) combinedValue);
            values[index] = (int) (combinedValue >> 32);
        }
    }

    private static void justSort(int[] values, float[] sidecar, long[] buffer, int length) {
        length = Math.min(length, Math.min(values.length, sidecar.length));
        assert buffer.length >= length : "buffer must at least " + length + " long, but it only is " + buffer.length;

        for (int i = 0; i < length; i++) {
            buffer[i] = (((long) values[i]) << 32) | (((long) Float.floatToRawIntBits(sidecar[i])) & 0xFFFF_FFFFL);
        }

        Arrays.sort(buffer, 0, length);
    }
}
