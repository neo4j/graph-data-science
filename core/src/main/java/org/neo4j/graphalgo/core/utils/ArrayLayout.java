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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.gds.annotation.ValueClass;

import java.util.Objects;

public final class ArrayLayout {

    /**
     * Constructs a new binary search tree using the Eytzinger layout.
     * Input must be sorted.
     *
     * @param input the sorted input data
     */
    public static long[] constructEytzinger(long[] input) {
        return constructEytzinger(input, 0, input.length);
    }

    /**
     * Constructs a new binary search tree using the Eytzinger layout.
     * Input must be sorted.
     *
     * @param input the sorted input data
     * @param offset where to start at in the input
     * @param length how many elements to use from the input
     */
    public static long[] constructEytzinger(long[] input, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, input.length);
        // position 0 is the result of a left-biased miss (needle is smaller than the smallest entry).
        // the actual values are stored 1-based
        var dest = new long[length + 1];
        dest[0] = -1;
        eytzinger(length, input, dest, offset, 1);
        return dest;
    }

    /**
     * Constructs a new binary search tree using the Eytzinger layout.
     * Input must be sorted.
     * A secondary array is permuted in the same fashion as the input array.
     *
     * @param input the sorted input data
     * @param secondary secondary values that are permuted as well
     */
    public static LayoutAndSecondary constructEytzinger(long[] input, int[] secondary) {
        if (secondary.length != input.length) {
            throw new IllegalArgumentException("Input arrays must be of same length");
        }
        // position 0 is the result of a left-biased miss (needle is smaller than the smallest entry).
        // the actual values are stored 1-based
        var dest = new long[input.length + 1];
        dest[0] = -1;
        var secondaryDest = new int[secondary.length];
        eytzingerWithSecondary(input.length, input, dest, 0, 1, secondary, secondaryDest);
        return ImmutableLayoutAndSecondary.of(dest, secondaryDest);
    }

    private static int eytzinger(int length, long[] source, long[] dest, int sourceIndex, int destIndex) {
        if (destIndex <= length) {
            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex);
            dest[destIndex] = source[sourceIndex++];
            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex + 1);
        }
        return sourceIndex;
    }

    private static int eytzingerWithSecondary(int length, long[] source, long[] dest, int sourceIndex, int destIndex, int[] secondarySource, int[] secondaryDest) {
        if (destIndex <= length) {
            sourceIndex = eytzingerWithSecondary(length, source, dest, sourceIndex, 2 * destIndex, secondarySource, secondaryDest);
            secondaryDest[destIndex - 1] = secondarySource[sourceIndex];
            dest[destIndex] = source[sourceIndex++];
            sourceIndex = eytzingerWithSecondary(length, source, dest, sourceIndex, 2 * destIndex + 1, secondarySource, secondaryDest);
        }
        return sourceIndex;
    }

    /**
     * Searches for the needle in the haystack, returning an index pointing at the needle.
     *
     * The array must be one constructed from {@link #constructEytzinger(long[])} or related.
     * Any other order of the array (e.g. sorted for binary search) will produce undefined results.
     *
     * Unlike {@link java.util.Arrays#binarySearch(long[], long)}, this method returns the index of the value
     * that is either equal to the needle or the next smallest one. There are no different results to signal whether
     * a value was found or not. If you need to know whether the value is contained in the array, you need to compare
     * the value against the array at the position of the returned index.
     * The index returned is the last index where the value is not larger than the needle.
     * This is also different from the j.u.Arrays method.
     * That one returns "the index of the first element greater than the key", that is the upper bound of the search.
     * Starting from that index upto the end of the array, all values are either equal to or greater than the needle.
     * In contrast, this method returns the lower bound.
     * Starting from 0 up to, and including, the returned index, all values are either less than or equal to the needle.
     *
     * @param haystack the input array sorted and constructed by {@link #constructEytzinger(long[])}
     * @param needle the needle to search for
     * @return the lower bound for the needle. Either the index of the needle if it was in the array
     *         or the index preceding the place where the needle would be. Note that, unlike the sibling method
     *         {@link java.util.Arrays#binarySearch(long[], long)}, the index returned *cannot* be used to change
     *         the contents of the search array.
     */
    public static int searchEytzinger(long[] haystack, long needle) {
        int index = 1;
        int length = haystack.length - 1;
        while (index <= length) {
            index = needle < haystack[index] ? index << 1 : (index << 1) + 1;
        }
        // The index is basically a record of the branches that we traversed in the tree,
        // where a 0 means that we took the right branch and a 1 for the left branch.
        // Once the index is out of bounds (i.e. index > length), we need to track back and
        // undo all the right branches that we took.
        return index >>> (1 + Integer.numberOfTrailingZeros(index));
    }

    @ValueClass
    public interface LayoutAndSecondary {
        long[] layout();

        int[] secondary();
    }

    private ArrayLayout() {}
}
