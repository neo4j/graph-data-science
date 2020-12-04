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
//        // position 0 is the result of a miss.
//        // the actual values are stored 1-based
//        var dest = new long[length + 1];
//        dest[0] = -1;
//        eytzinger(length, input, dest, offset, 1);
        var dest = new long[length];
        eytzinger(length, input, dest, offset, 0);
        return dest;
    }

    private static int eytzinger(int length, long[] source, long[] dest, int sourceIndex, int destIndex) {
        if (destIndex < length) {
            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex + 1);
            dest[destIndex] = source[sourceIndex++];
            sourceIndex = eytzinger(length, source, dest, sourceIndex, 2 * destIndex + 2);
        }
        return sourceIndex;
    }

    public static int search_eytzinger(long[] haystack, long needle) {
        int index = 0;
        int length = haystack.length;
        while (index < length) {
            index = needle < haystack[index] ? ((index << 1) + 1) : ((index << 1) + 2);
        }
        var ctz = Integer.numberOfTrailingZeros(index + 1);
        var ffs = ctz + 1;
        var idx = (index + 1) >>> ffs;
        return idx - 1;
    }

    private ArrayLayout() {}
}
