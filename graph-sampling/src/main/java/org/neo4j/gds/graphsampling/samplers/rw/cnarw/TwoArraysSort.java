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
package org.neo4j.gds.graphsampling.samplers.rw.cnarw;

import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;

public final class TwoArraysSort {
    private TwoArraysSort() {}

    /**
     * Sort two arrays simultaneously based on values of the first (long) array.
     * E.g. {[4, 1, 8], [0.5, 1.9, 0.9]} -> {[1, 4, 8], [1,9, 0.5, 0.9]}
     *
     * @param longArray   Array of long values (e.g. neighbours ids)
     * @param doubleArray Array of double values (e.g. neighbours weighs)
     * @param length      Number of values to sort
     */
    static void sortDoubleArrayByLongValues(long[] longArray, double[] doubleArray, int length) {
        assert longArray.length >= length;
        assert doubleArray.length >= length;
        if (checkIfSorted(longArray, length)) {
            return;
        }
        var order = IndirectSort.mergesort(0, length, new AscendingLongComparator(longArray));
        reorder(order, longArray, doubleArray, length);
    }

    private static void reorder(int[] order, long[] longArray, double[] doubleArray, int length) {
        for (int i = 0; i < length; i++) {
            while (order[i] != i) {
                int idx = order[order[i]];
                var longVal = longArray[order[i]];
                var doubleVal = doubleArray[order[i]];

                longArray[order[i]] = longArray[i];
                doubleArray[order[i]] = doubleArray[i];
                order[order[i]] = order[i];

                order[i] = idx;
                longArray[i] = longVal;
                doubleArray[i] = doubleVal;
            }
        }
    }

    private static boolean checkIfSorted(long[] longArray, int length) {
        for (int i = 0; i < length - 1; i++) {
            if (longArray[i] > longArray[i + 1])
                return false;
        }
        return true;
    }

    public static class AscendingLongComparator implements IndirectComparator {
        private final long[] array;

        AscendingLongComparator(long[] array) {
            this.array = array;
        }

        public int compare(int indexA, int indexB) {
            final long a = array[indexA];
            final long b = array[indexB];

            if (a < b)
                return -1;
            if (a > b)
                return 1;
            return 0;
        }
    }
}
