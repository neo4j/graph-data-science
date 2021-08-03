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
package org.neo4j.gds.similarity.knn;

import java.util.Arrays;

final class NeighborSort {

    private static final int RADIX = 8;
    private static final int HIST_SIZE = 1 << RADIX;

    public static int[] newHistogram(int length) {
        return new int[Math.max(length, 1 + HIST_SIZE)];
    }

    public static long[] newCopy(long[] data) {
        return new long[data.length];
    }

    public static void radixSort(long[] data, long[] copy, int[] histogram, int length) {
        radixSort(data, copy, histogram, length, 0);
    }

    private static void radixSort(long[] data, long[] copy, int[] histogram, int length, int shift) {
        int hlen = Math.min(HIST_SIZE, histogram.length - 1);
        int dlen = Math.min(length, Math.min(data.length, copy.length));

        long hiBits, loMask = 0xFFL << shift, hiMask = -(0x100L << shift);
        int maxHistIndex, histIndex, out;

        while (shift < Long.SIZE) {
            Arrays.fill(histogram, 0, 1 + hlen, 0);
            maxHistIndex = 0;
            hiBits = 0L;

            for (int i = 0; i < dlen; i += 2) {
                hiBits |= data[i] & hiMask;
                histIndex = (int) ((data[i] & loMask) >>> shift);
                maxHistIndex |= histIndex;
                histogram[1 + histIndex] += 2;
            }

            if (hiBits == 0L && maxHistIndex == 0) {
                return;
            }

            if (maxHistIndex != 0) {
                for (int i = 0; i < hlen; ++i) {
                    histogram[i + 1] += histogram[i];
                }

                for (int i = 0; i < dlen; i += 2) {
                    out = histogram[(int) ((data[i] & loMask) >>> shift)] += 2;
                    copy[out - 2] = data[i];
                    copy[out - 1] = data[1 + i];
                }

                System.arraycopy(copy, 0, data, 0, dlen);
            }

            shift += RADIX;
            loMask <<= RADIX;
            hiMask <<= RADIX;
        }
    }

    private NeighborSort() {}
}
