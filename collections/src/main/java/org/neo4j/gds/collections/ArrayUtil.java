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

import org.neo4j.gds.mem.MemoryUsage;

import java.util.Arrays;

public final class ArrayUtil {

    private static final int LINEAR_SEARCH_LIMIT = 64;

    public static boolean binarySearch(int[] arr, int length, int key) {
        int low = 0;
        int high = length - 1;
        while (high - low > LINEAR_SEARCH_LIMIT) {
            int mid = (low + high) >>> 1;
            int midVal = arr[mid];
            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return true;
        }
        return linearSearch2(arr, low, high, key);

    }

    /**
     * Similar to {@link Arrays#binarySearch(long[], int, int, long)}, but
     * returns the index of the first occurrence of {@code key} in {@code a}
     * if there are multiple occurrences.
     *
     * @return index of the first occurrence of the search key, if it is contained in the array;
     *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>. The <i>insertion point</i>
     *         is defined as the point at which the key would be inserted into the array:
     *         the index of the first element greater than the key, or {@code a.length} if all
     *         elements in the array are less than the specified key. Note that this guarantees
     *         that the return value will be &gt;= 0 if and only if the key is found.
     */
    public static int binarySearchFirst(long[] a, int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = a[mid];

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else if (mid > 0 && a[mid - 1] == key) // key found, but not first index
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    /**
     * Similar to {@link Arrays#binarySearch(long[], int, int, long)}, but
     * returns the index of the last occurrence of {@code key} in {@code a}
     * if there are multiple occurrences.
     *
     * @return index of the last occurrence of the search key, if it is contained in the array;
     *         otherwise, <code>(-(<i>insertion point</i>) - 1)</code>. The <i>insertion point</i>
     *         is defined as the point at which the key would be inserted into the array:
     *         the index of the first element greater than the key, or {@code a.length} if all
     *         elements in the array are less than the specified key. Note that this guarantees
     *         that the return value will be &gt;= 0 if and only if the key is found.
     */
    public static int binarySearchLast(long[] a, int fromIndex, int toIndex, long key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = a[mid];

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else if (mid < toIndex - 1 && a[mid + 1] == key) // key found, but not last index
                low = mid + 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    public static int binarySearchIndex(int[] arr, int length, int key) {
        int low = 0;
        int high = length - 1;
        while (high - low > LINEAR_SEARCH_LIMIT) {
            int mid = (low + high) >>> 1;
            int midVal = arr[mid];
            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid;
        }
        return linearSearch2index(arr, low, high, key);

    }

    // TODO eval
    public static boolean linearSearch2(int[] arr, int low, int high, int key) {
        for (int i = low; i <= high; i++) {
            if (arr[i] == key) return true;
            if (arr[i] > key) return false;
        }
        return false;
    }

    // TODO eval
    public static int linearSearch2index(int[] arr, int low, int high, int key) {
        for (int i = low; i <= high; i++) {
            if (arr[i] == key) return i;
            if (arr[i] > key) return -i - 1;
        }
        return (-(high) - 1);
    }

    public static boolean linearSearch(int[] arr, int length, int key) {
        int i = 0;
        for (; i < length - 4; i += 4) {
            if (arr[i] == key) return true;
            if (arr[i + 1] == key) return true;
            if (arr[i + 2] == key) return true;
            if (arr[i + 3] == key) return true;
        }
        for (; i < length; i++) {
            if (arr[i] == key) {
                return true;
            }
        }
        return false;
    }

    public static int linearSearchIndex(int[] arr, int length, int key) {
        int i = 0;
        for (; i < length - 4; i += 4) {
            if (arr[i] == key) return i;
            if (arr[i + 1] == key) return i + 1;
            if (arr[i + 2] == key) return i + 2;
            if (arr[i + 3] == key) return i + 3;
        }
        for (; i < length; i++) {
            if (arr[i] == key) {
                return i;
            }
        }
        return -length - 1;
    }

    private static boolean linearSearch(int[] arr, int low, int high, int key) {
        int i = low;
        for (; i < high - 3; i += 4) {
            if (arr[i] > key) return false;
            if (arr[i] == key) return true;
            if (arr[i + 1] == key) return true;
            if (arr[i + 2] == key) return true;
            if (arr[i + 3] == key) return true;
        }
        for (; i <= high; i++) {
            if (arr[i] == key) {
                return true;
            }
        }
        return false;
    }

    /**
     * Find the index where {@code (ids[idx] <= id) && (ids[idx + 1] > id)}.
     * The result differs from that of {@link java.util.Arrays#binarySearch(long[], long)}
     * in that this method returns a positive index even if the array does not
     * directly contain the searched value.
     * It returns -1 iff the value is smaller than the smallest one in the array.
     */
    public static int binaryLookup(long id, long[] ids) {
        int length = ids.length;

        int low = 0;
        int high = length - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = ids[mid];

            if (midVal < id) {
                low = mid + 1;
            } else if (midVal > id) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low - 1;
    }

    public static double[] fill(double value, int length) {
        double[] data = new double[length];
        Arrays.fill(data, value);
        return data;
    }

    /**
     * Finds whether an array contains a given value.
     * Linear scans the array and returns true on finding any value equal to the argument.
     */
    public static boolean contains(long[] array, long value) {
        for (long l : array) {
            if (l == value) {
                return true;
            }
        }
        return false;
    }

    private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - MemoryUsage.BYTES_ARRAY_HEADER;

    /**
     * This is copied from Apache Lucene which is licensed under Apache License, Version 2.0.
     * <p>
     * See <a href="https://github.com/apache/lucene/blob/367cd2ea95b065f8f1a04e870e50133c0df39c81/lucene/core/src/java/org/apache/lucene/util/ArrayUtil.java#L134">source</a>
     * <p>
     * Returns an array size &gt;= minTargetSize, generally over-allocating exponentially to achieve
     * amortized linear-time cost as the array grows.
     *
     * <p>NOTE: this was originally borrowed from Python 2.4.2 listobject.c sources (attribution in
     * LICENSE.txt), but has now been substantially changed based on discussions from java-dev thread
     * with subject "Dynamic array reallocation algorithms", started on Jan 12 2010.
     *
     * @param minTargetSize   Minimum required value to be returned.
     * @param bytesPerElement Bytes used by each element of the array.
     */
    public static int oversize(int minTargetSize, int bytesPerElement) {

        if (minTargetSize < 0) {
            // catch usage that accidentally overflows int
            throw new IllegalArgumentException("invalid array size " + minTargetSize);
        }

        if (minTargetSize == 0) {
            // wait until at least one element is requested
            return 0;
        }

        if (minTargetSize > MAX_ARRAY_LENGTH) {
            throw new IllegalArgumentException(
                "requested array size "
                + minTargetSize
                + " exceeds maximum array in java ("
                + MAX_ARRAY_LENGTH
                + ")");
        }

        // asymptotic exponential growth by 1/8th, favors
        // spending a bit more CPU to not tie up too much wasted
        // RAM:
        int extra = minTargetSize >> 3;

        if (extra < 3) {
            // for very small arrays, where constant overhead of
            // realloc is presumably relatively high, we grow
            // faster
            extra = 3;
        }

        int newSize = minTargetSize + extra;

        // add 7 to allow for worst case byte alignment addition below:
        if (newSize + 7 < 0 || newSize + 7 > MAX_ARRAY_LENGTH) {
            // int overflowed, or we exceeded the maximum array length
            return MAX_ARRAY_LENGTH;
        }

        if (MemoryUsage.BYTES_OBJECT_REF == 8) {
            // round up to 8 byte alignment in 64bit env
            switch (bytesPerElement) {
                case 4:
                    // round up to multiple of 2
                    return (newSize + 1) & 0x7ffffffe;
                case 2:
                    // round up to multiple of 4
                    return (newSize + 3) & 0x7ffffffc;
                case 1:
                    // round up to multiple of 8
                    return (newSize + 7) & 0x7ffffff8;
                case 8:
                    // no rounding
                default:
                    // odd (invalid?) size
                    return newSize;
            }
        } else {
            // In 32bit jvm, it's still 8-byte aligned,
            // but the array header is 12 bytes, not a multiple of 8.
            // So saving 4,12,20,28... bytes of data is the most cost-effective.
            switch (bytesPerElement) {
                case 1:
                    // align with size of 4,12,20,28...
                    return ((newSize + 3) & 0x7ffffff8) + 4;
                case 2:
                    // align with size of 6,10,14,18...
                    return ((newSize + 1) & 0x7ffffffc) + 2;
                case 4:
                    // align with size of 5,7,9,11...
                    return (newSize & 0x7ffffffe) + 1;
                case 8:
                    // no processing required
                default:
                    // odd (invalid?) size
                    return newSize;
            }
        }
    }

    /**
     * Huge version of Lucene oversize for arrays.
     * see org.apache.lucene.util.ArrayUtil#oversize(int, int)
     */
    public static long oversizeHuge(long minTargetSize, int bytesPerElement) {

        if (minTargetSize == 0) {
            // wait until at least one element is requested
            return 0;
        }

        // asymptotic exponential growth by 1/8th, favors
        // spending a bit more CPU to not tie up too much wasted
        // RAM:
        long extra = minTargetSize >> 3;

        if (extra < 3) {
            // for very small arrays, where constant overhead of
            // realloc is presumably relatively high, we grow
            // faster
            extra = 3;
        }

        long newSize = minTargetSize + extra;

        if (MemoryUsage.BYTES_OBJECT_REF == 8) {
            // round up to 8 byte alignment to match JVM pointer size
            switch (bytesPerElement) {
                case 4:
                    // round up to multiple of 2
                    return (newSize + 1) & 0x7FFF_FFFE;
                case 2:
                    // round up to multiple of 4
                    return (newSize + 3) & 0x7FFF_FFFC;
                case 1:
                    // round up to multiple of 8
                    return (newSize + 7) & 0x7FFF_FFF8;
                case 8:
                    // no rounding
                default:
                    return newSize;
            }
        } else {
            // round up to 4 byte alignment to match JVM pointer size
            switch (bytesPerElement) {
                case 2:
                    // round up to multiple of 2
                    return (newSize + 1) & 0x7FFFFFFE;
                case 1:
                    // round up to multiple of 4
                    return (newSize + 3) & 0x7FFFFFFC;
                case 4:
                case 8:
                    // no rounding
                default:
                    return newSize;
            }
        }
    }


    private ArrayUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
