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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

final class RadixSortTest {

    @Test
    void testSortBySource() {
        var data = testData();
        RadixSort.radixSort(
            data.data,
            RadixSort.newCopy(data.data),
            data.additionalData1,
            RadixSort.newCopy(data.additionalData1),
            data.additionalData2,
            RadixSort.newCopy(data.additionalData2),
            RadixSort.newHistogram(0),
            data.data.length
        );
        var expected = expectedBySource();
        assertArrayEquals(expected.data, data.data);
        assertArrayEquals(expected.additionalData1, data.additionalData1);
        assertArrayEquals(expected.additionalData2, data.additionalData2);
    }

    @Test
    void testSortByTarget() {
        var data = testData();
        RadixSort.radixSort2(
            data.data,
            RadixSort.newCopy(data.data),
            data.additionalData1,
            RadixSort.newCopy(data.additionalData1),
            data.additionalData2,
            new Long[data.additionalData2.length],
            RadixSort.newHistogram(0),
            data.data.length
        );
        var expected = expectedByTarget();
        assertArrayEquals(expected.data, data.data);
        assertArrayEquals(expected.additionalData1, data.additionalData1);
        assertArrayEquals(expected.additionalData2, data.additionalData2);
    }

    @Test
    void sortLargeBatch() {
        long[] data = new long[3840];
        long[] additionalData1 = new long[1920];
        Long[] additionalData2 = new Long[1920];
        int index = 0;
        int additionalIndex = 0;
        for (long i = 0L; i < 920L; i++) {
            data[index++] = 0L;
            data[index++] = i + 1L;
            additionalData1[additionalIndex] = i + 1240L;
            additionalData2[additionalIndex++] = -1L;
        }
        for (long i = 1L; i < 1000L; i++) {
            data[index++] = i;
            data[index++] = i + 1L;
            additionalData1[additionalIndex] = i + 239L;
            additionalData2[additionalIndex++] = -1L;
        }
        data[index++] = 1000L;
        data[index] = 1L;
        additionalData1[additionalIndex] = 1239L;
        additionalData2[additionalIndex] = -1L;


        long[] expectedData = new long[3840];
        long[] expectedAdditionalData1 = new long[1920];
        Long[] expectedAdditionalData2 = new Long[1920];

        index = 0;
        additionalIndex = 0;

        expectedData[index++] = 1L;
        expectedData[index++] = 0L;
        expectedAdditionalData1[additionalIndex] = 1240L;
        expectedAdditionalData2[additionalIndex++] = -1L;

        expectedData[index++] = 1L;
        expectedData[index++] = 1000L;
        expectedAdditionalData1[additionalIndex] = 1239L;
        expectedAdditionalData2[additionalIndex++] = -1L;

        for (long i = 1L; i < 920L; i++) {
            expectedData[index++] = i + 1L;
            expectedData[index++] = 0L;
            expectedAdditionalData1[additionalIndex] = i + 1240L;
            expectedAdditionalData2[additionalIndex++] = -1L;

            expectedData[index++] = i + 1L;
            expectedData[index++] = i;
            expectedAdditionalData1[additionalIndex] = i + 239L;
            expectedAdditionalData2[additionalIndex++] = -1L;
        }

        for (long i = 920L; i < 1000L; i++) {
            expectedData[index++] = i + 1L;
            expectedData[index++] = i;
            expectedAdditionalData1[additionalIndex] = i + 239L;
            expectedAdditionalData2[additionalIndex++] = -1L;
        }

        RadixSort.radixSort2(
            data,
            new long[3840],
            additionalData1,
            new long[1920],
            additionalData2,
            new Long[1920],
            RadixSort.newHistogram(0),
            3840
        );
        assertArrayEquals(expectedData, data);
        assertArrayEquals(expectedAdditionalData1, additionalData1);
        assertArrayEquals(expectedAdditionalData2, additionalData2);
    }

    private static Data<Long> testData() {
        //@formatter:off
        var data = new long[]{
                1L << 25 | 25L,  1,   1L << 16 |  1L,  4,         0L,  7,   1L << 25 | 10L, 11,
                           25L, 14,   1L << 16 | 10L, 17,   1L << 16, 20,              10L, 23,
                1L << 52 | 25L,  1,   1L << 44 |  1L,  4,   1L << 34,  7,   1L << 52 | 10L, 11,
                1L << 34 | 25L, 14,   1L << 44 | 10L, 17,   1L << 44, 20,   1L << 34 | 10L, 23,
                1L << 63 | 25L,  1,   1L << 62 |  1L,  4,   1L << 57,  7,   1L << 63 | 10L, 11,
                1L << 57 | 25L, 14,   1L << 62 | 10L, 17,   1L << 62, 20,   1L << 57 | 10L, 23,
        };
        var additionalData1 = new long[]{
                 2,  5,  8, 12,
                15, 18, 21, 24,
                 2,  5,  8, 12,
                15, 18, 21, 24,
                 2,  5,  8, 12,
                15, 18, 21, 24,
        };
        var additionalData2 = new Long[]{
                 3L,  6L,  9L, 13L,
                16L, 19L, 22L, 25L,
                 3L,  6L,  9L, 13L,
                16L, 19L, 22L, 25L,
                 3L,  6L,  9L, 13L,
                16L, 19L, 22L, 25L,
        };
        //@formatter:on
        return new Data<>(data, additionalData1, additionalData2);
    }

    @SuppressWarnings("checkstyle:NoWhitespaceBefore")
    private static Data<Long> expectedBySource() {
        //@formatter:off
        var data = new long[]{
                            0L,  7,              10L, 23,              25L, 14,   1L << 16      , 20,
                1L << 16 |  1L,  4,   1L << 16 | 10L, 17,   1L << 25 | 10L, 11,   1L << 25 | 25L,  1,
                1L << 34      ,  7,   1L << 34 | 10L, 23,   1L << 34 | 25L, 14,   1L << 44      , 20,
                1L << 44 |  1L,  4,   1L << 44 | 10L, 17,   1L << 52 | 10L, 11,   1L << 52 | 25L,  1,
                1L << 57      ,  7,   1L << 57 | 10L, 23,   1L << 57 | 25L, 14,   1L << 62      , 20,
                1L << 62 |  1L,  4,   1L << 62 | 10L, 17,   1L << 63 | 10L, 11,   1L << 63 | 25L,  1,
        };
        var additionalData1 = new long[]{
                8, 24, 15, 21,
                5, 18, 12,  2,
                8, 24, 15, 21,
                5, 18, 12,  2,
                8, 24, 15, 21,
                5, 18, 12,  2,
        };
        var additionalData2 = new Long[]{
                9L, 25L, 16L, 22L,
                6L, 19L, 13L,  3L,
                9L, 25L, 16L, 22L,
                6L, 19L, 13L,  3L,
                9L, 25L, 16L, 22L,
                6L, 19L, 13L,  3L,
        };
        //@formatter:on
        return new Data<>(data, additionalData1, additionalData2);
    }

    @SuppressWarnings("checkstyle:NoWhitespaceBefore")
    private static Data<Long> expectedByTarget() {
        //@formatter:off
        var data = new long[]{
                 1, 1L << 25 | 25L,    1, 1L << 52 | 25L,    1, 1L << 63 | 25L,    4, 1L << 16 |  1L,
                 4, 1L << 44 |  1L,    4, 1L << 62 |  1L,    7,             0L,    7, 1L << 34      ,
                 7, 1L << 57      ,   11, 1L << 25 | 10L,   11, 1L << 52 | 10L,   11, 1L << 63 | 10L,
                14,            25L,   14, 1L << 34 | 25L,   14, 1L << 57 | 25L,   17, 1L << 16 | 10L,
                17, 1L << 44 | 10L,   17, 1L << 62 | 10L,   20, 1L << 16      ,   20, 1L << 44      ,
                20, 1L << 62      ,   23,            10L,   23, 1L << 34 | 10L,   23, 1L << 57 | 10L,
        };
        var additionalData1 = new long[]{
                 2,  2,  2,  5,
                 5,  5,  8,  8,
                 8, 12, 12, 12,
                15, 15, 15, 18,
                18, 18, 21, 21,
                21, 24, 24, 24,
        };
        var additionalData2 = new Long[]{
                 3L,  3L,  3L,  6L,
                 6L,  6L,  9L,  9L,
                 9L, 13L, 13L, 13L,
                16L, 16L, 16L, 19L,
                19L, 19L, 22L, 22L,
                22L, 25L, 25L, 25L,
        };
        //@formatter:on
        return new Data<>(data, additionalData1, additionalData2);
    }

    static final class Data<T> {
        final long[] data;
        final long[] additionalData1;
        final T[] additionalData2;

        Data(long[] data, long[] additionalData1, T[] additionalData2) {
            this.data = data;
            this.additionalData1 = additionalData1;
            this.additionalData2 = additionalData2;
        }
    }
}
