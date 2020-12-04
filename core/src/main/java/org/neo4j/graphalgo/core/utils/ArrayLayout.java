/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import java.util.Objects;

public final class ArrayLayout {

    /**
     * Constructs a new binary search tree using the Eytzinger layout.
     * Input must be sorted.
     *
     * @param input the sorted input data
     */
    public static long[] construct_eytzinger(long[] input) {
        return construct_eytzinger(input, 0, input.length);
    }

    /**
     * Constructs a new binary search tree using the Eytzinger layout.
     * Input must be sorted.
     *
     * @param input the sorted input data
     * @param length how many elements to use from the input
     */
    public static long[] construct_eytzinger(long[] input, int length) {
        return construct_eytzinger(input, 0, length);
    }

    /**
     * Constructs a new binary search tree using the Eytzinger layout.
     * Input must be sorted.
     *
     * @param input the sorted input data
     * @param offset where to start at in the input
     * @param length how many elements to use from the input
     */
    public static long[] construct_eytzinger(long[] input, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, input.length);
        // position 0 is the result of a left-biased miss (needle is smaller than the smallest entry).
        // the actual values are stored 1-based
        var dest = new long[length + 1];
        dest[0] = -1;
        eytzinger(length, input, dest, offset, 1);
//        var dest = new long[length];
//        eytzinger(length, input, dest, offset, 0);
        return dest;
    }

    private static int eytzinger(int length, long[] source, long[] dest, int sourceIndex, int destIndex) {
        if (destIndex <= length) {
            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex);
//            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex + 1);
            dest[destIndex] = source[sourceIndex++];
            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex + 1);
//            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex + 2);
        }
        return sourceIndex;
    }

    /**
     * Searches for the needle in the haystack, returning an index pointing at the needle.
     *
     * The array must be one constructed from {@link #construct_eytzinger(long[])} or related.
     * Any other order of the array (e.g. sorted for binary search) will produce undefined results.
     *
     * Unlike {@link java.util.Arrays#binarySearch(long[], long)}, this method returns the index of the value
     * that is either equal to the needle or the next smallest one. There are no different results to signal whether
     * a value was found or not.
     * The index returned is the last index where the value is not larger than the needle.
     * This is also different from the j.u.Arrays method.
     * That one returns "the index of the first element greater than the key", that is the upper bound of the search.
     * Starting from that index upto the end of the array, all values are either equal to or greater than the needle.
     * In contrast, this method returns the lower bound.
     * Starting from 0 upto, and including, the returned index, all values are either less than or equal to the needle.
     *
     * @param haystack the input array sorted and constructed by {@link #construct_eytzinger(long[])}
     * @param needle the needle to search for
     * @return the lower bound for the needle. Either the index of the needle if it was in the array
     *         or the index preceding the place where the needle would be. Note that, unlike the sibling method
     *         {@link java.util.Arrays#binarySearch(long[], long)}, the index returned *cannot* be used to change
     *         the contents of the search array.
     */
    public static int search_eytzinger(long[] haystack, long needle) {
        int index = 1;
        int length = haystack.length - 1;
        while (index <= length) {
            index = needle < haystack[index] ? index << 1 : (index << 1) + 1;
//            index = (index << 1) + (needle < haystack[index] ? 0 : 1);
        }
        return index >>> (1 + Integer.numberOfTrailingZeros(index));
    }

//    public static int search_eytzinger(long[] haystack, long needle) {
//        int index = 0;
//        int length = haystack.length;
//        while (index < length) {
//            index = needle < haystack[index] ? ((index << 1) + 1) : ((index << 1) + 2);
//        }
//        var ctz = Integer.numberOfTrailingZeros(index + 1);
//        var ffs = ctz + 1;
//        var idx = (index + 1) >>> ffs;
//        return idx - 1;
//    }

    private ArrayLayout() {}
}
