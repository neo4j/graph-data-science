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
package org.neo4j.gds.core.loading;

import java.lang.reflect.Array;
import java.util.Arrays;

public final class RadixSort {

    private static final int RADIX = 8;
    private static final int HIST_SIZE = 1 << RADIX;

    public static int[] newHistogram(int length) {
        return new int[Math.max(length, 1 + HIST_SIZE)];
    }

    public static long[] newCopy(long[] data) {
        return new long[data.length];
    }

    public static <T> T[] newCopy(T[] data) {
        return (T[]) Array.newInstance(data.getClass().getComponentType(), data.length);
    }

    public static <T> void radixSort(
        long[] data,
        long[] dataCopy,
        long[] additionalData1,
        long[] additionalCopy1,
        T[] additionalData2,
        T[] additionalCopy2,
        int[] histogram,
        int length
    ) {
        radixSort(
            data,
            dataCopy,
            additionalData1,
            additionalCopy1,
            additionalData2,
            additionalCopy2,
            histogram,
            length,
            0
        );
    }

    private static <T> void radixSort(
        long[] data,
        long[] dataCopy,
        long[] additionalData1,
        long[] additionalCopy1,
        T[] additionalData2,
        T[] additionalCopy2,
        int[] histogram,
        int length,
        int shift
    ) {
        int hlen = Math.min(HIST_SIZE, histogram.length - 1);
        int dlen = Math.min(length, Math.min(data.length, dataCopy.length));

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
                    dataCopy[out - 2] = data[i];
                    dataCopy[out - 1] = data[1 + i];
                    additionalCopy1[(out - 2) / 2] = additionalData1[i / 2];
                    additionalCopy2[(out - 2) / 2] = additionalData2[i / 2];
                }

                System.arraycopy(dataCopy, 0, data, 0, dlen);
                System.arraycopy(additionalCopy1, 0, additionalData1, 0, dlen / 2);
                System.arraycopy(additionalCopy2, 0, additionalData2, 0, dlen / 2);
            }

            shift += RADIX;
            loMask <<= RADIX;
            hiMask <<= RADIX;
        }
    }

    public static <T> void radixSort2(
        long[] data,
        long[] dataCopy,
        long[] additionalData1,
        long[] additionalCopy1,
        T[] additionalData2,
        T[] additionalCopy2,
        int[] histogram,
        int length
    ) {
        radixSort2(
            data,
            dataCopy,
            additionalData1,
            additionalCopy1,
            additionalData2,
            additionalCopy2,
            histogram,
            length,
            0
        );
    }

    private static <T> void radixSort2(
        long[] data,
        long[] dataCopy,
        long[] additionalData1,
        long[] additionalCopy1,
        T[] additionalData2,
        T[] additionalCopy2,
        int[] histogram,
        int length,
        int shift
    ) {
        int hlen = Math.min(HIST_SIZE, histogram.length - 1);
        int dlen = Math.min(length, Math.min(data.length, dataCopy.length));
        Arrays.fill(histogram, 0, hlen, 0);

        long loMask = 0xFFL << shift;
        for (int i = 0; i < dlen; i += 2) {
            histogram[1 + (int) ((data[1 + i] & loMask) >>> shift)] += 2;
        }

        for (int i = 0; i < hlen; ++i) {
            histogram[i + 1] += histogram[i];
        }

        int out;
        for (int i = 0; i < dlen; i += 2) {
            out = histogram[(int) ((data[1 + i] & loMask) >>> shift)] += 2;
            dataCopy[out - 2] = data[1 + i];
            dataCopy[out - 1] = data[i];
            additionalCopy1[(out - 2) / 2] = additionalData1[i / 2];
            additionalCopy2[(out - 2) / 2] = additionalData2[i / 2];
        }

        System.arraycopy(dataCopy, 0, data, 0, dlen);
        System.arraycopy(additionalCopy1, 0, additionalData1, 0, dlen / 2);
        System.arraycopy(additionalCopy2, 0, additionalData2, 0, dlen / 2);

        radixSort(
            data,
            dataCopy,
            additionalData1,
            additionalCopy1,
            additionalData2,
            additionalCopy2,
            histogram,
            length,
            shift + RADIX
        );
    }

    private RadixSort() {}
}
