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

import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;

public final class TwoArraysSort {
    private TwoArraysSort() {}

    /**
     * Sort two arrays simultaneously based on values of the first (long) array.
     * E.g. {[4, 1, 8], [0.5, 1.9, 0.9]} -&gt; {[1, 4, 8], [1,9, 0.5, 0.9]}
     *
     * @param longArray   Array of long values (e.g. neighbours ids)
     * @param doubleArray Array of double values (e.g. neighbours weighs)
     * @param length      Number of values to sort
     */
    public static void sortDoubleArrayByLongValues(long[] longArray, double[] doubleArray, int length) {
        assert longArray.length >= length;
        assert doubleArray.length >= length;
        var order = IndirectSort.mergesort(0, length, new AscendingLongComparator(longArray));
        reorder(order, longArray, doubleArray, length);
    }

    static void reorder(int[] order, long[] longArray, double[] doubleArray, int length) {
        for (int i = 0; i < length; i++) {
            var initV = longArray[i];
            var initW = doubleArray[i];
            var currIdx = i;
            while (order[currIdx] !=i){
                var nci = order[currIdx];
                longArray[currIdx] = longArray[nci];
                doubleArray[currIdx] = doubleArray[nci];
                order[currIdx] = currIdx;
                currIdx = nci;
            }
            longArray[currIdx] = initV;
            doubleArray[currIdx] = initW;
            order[currIdx] = currIdx;
        }
    }

    public static class AscendingLongComparator implements IndirectComparator {
        private final long[] array;

        AscendingLongComparator(long[] array) {
            this.array = array;
        }

        public int compare(int indexA, int indexB) {
            final long a = array[indexA];
            final long b = array[indexB];

            return Long.compare(a, b);
        }
    }
}
