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
package org.neo4j.gds.core.utils;

import java.util.Arrays;


public final class ArrayUtil {

    public static final int LINEAR_SEARCH_LIMIT = 64;

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

    private ArrayUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
