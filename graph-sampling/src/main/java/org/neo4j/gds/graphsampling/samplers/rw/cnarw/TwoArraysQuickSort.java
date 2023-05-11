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

public class TwoArraysQuickSort {
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
        quickSortLongsWithDoubles(longArray, doubleArray, 0, length - 1);
    }

    private static void quickSortLongsWithDoubles(long[] longArray, double[] doubleArray, int lo, int hi) {
        if (lo >= hi) {
            return;
        }
        int p = partition(longArray, doubleArray, lo, hi);
        quickSortLongsWithDoubles(longArray, doubleArray, lo, p - 1);
        quickSortLongsWithDoubles(longArray, doubleArray, p + 1, hi);
    }

    private static int partition(long[] longArray, double[] doubleArray, int lo, int hi) {
        long pivot = longArray[hi];
        int i = lo;
        for (int j = lo; j < hi; j++) {
            if (longArray[j] < pivot) {
                swap(longArray, doubleArray, i, j);
                i++;
            }
        }
        swap(longArray, doubleArray, i, hi);
        return i;
    }

    private static void swap(long[] l, double[] d, int i, int j) {
        long tempLong = l[i];
        l[i] = l[j];
        l[j] = tempLong;

        double tempDouble = d[i];
        d[i] = d[j];
        d[j] = tempDouble;
    }
}
