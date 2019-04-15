/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.huge.loader;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public final class RadixSortTest {

    @Test
    public void testSortBySource() {
        long[] data = testData();
        RadixSort.radixSort(data, RadixSort.newCopy(data), RadixSort.newHistogram(0), data.length);
        assertArrayEquals(expectedBySource(), data);
    }

    @Test
    public void testSortByTarget() {
        long[] data = testData();
        RadixSort.radixSort2(data, RadixSort.newCopy(data), RadixSort.newHistogram(0), data.length);
        assertArrayEquals(expectedByTarget(), data);
    }

    @Test
    public void sortLargeBatch() {
        long[] testcase = new long[7680];
        int index = 0;
        for (long i = 0L; i < 920L; i++) {
            testcase[index++] = 0L;
            testcase[index++] = i + 1L;
            testcase[index++] = i + 1240L;
            testcase[index++] = -1L;
        }
        for (long i = 1L; i < 1000L; i++) {
            testcase[index++] = i;
            testcase[index++] = i + 1L;
            testcase[index++] = i + 239L;
            testcase[index++] = -1L;
        }
        testcase[index++] = 1000L;
        testcase[index++] = 1L;
        testcase[index++] = 1239L;
        testcase[index] = -1L;


        long[] expected = new long[7680];
        index = 0;

        expected[index++] = 1L;
        expected[index++] = 0L;
        expected[index++] = 1240L;
        expected[index++] = -1L;

        expected[index++] = 1L;
        expected[index++] = 1000L;
        expected[index++] = 1239L;
        expected[index++] = -1L;

        for (long i = 1L; i < 920L; i++) {
            expected[index++] = i + 1L;
            expected[index++] = 0L;
            expected[index++] = i + 1240L;
            expected[index++] = -1L;

            expected[index++] = i + 1L;
            expected[index++] = i;
            expected[index++] = i + 239L;
            expected[index++] = -1L;
        }

        for (long i = 920L; i < 1000L; i++) {
            expected[index++] = i + 1L;
            expected[index++] = i;
            expected[index++] = i + 239L;
            expected[index++] = -1L;
        }

        RadixSort.radixSort2(testcase, new long[7680], new int[7680], 7680);
        assertArrayEquals(expected, testcase);
    }

    private static long[] testData() {
        //@formatter:off
        return new long[]{
                1L << 25 | 25L,  1,  2 , 3,   1L << 16 |  1L,  4,  5,  6,         0L,  7,  8,  9,   1L << 25 | 10L, 11, 12, 13,
                           25L, 14, 15, 16,   1L << 16 | 10L, 17, 18, 19,   1L << 16, 20, 21, 22,              10L, 23, 24, 25,
                1L << 52 | 25L,  1,  2,  3,   1L << 44 |  1L,  4,  5,  6,   1L << 34,  7,  8,  9,   1L << 52 | 10L, 11, 12, 13,
                1L << 34 | 25L, 14, 15, 16,   1L << 44 | 10L, 17, 18, 19,   1L << 44, 20, 21, 22,   1L << 34 | 10L, 23, 24, 25,
                1L << 63 | 25L,  1,  2,  3,   1L << 62 |  1L,  4,  5,  6,   1L << 57,  7,  8,  9,   1L << 63 | 10L, 11, 12, 13,
                1L << 57 | 25L, 14, 15, 16,   1L << 62 | 10L, 17, 18, 19,   1L << 62, 20, 21, 22,   1L << 57 | 10L, 23, 24, 25,
        };
        //@formatter:on
    }

    private static long[] expectedBySource() {
        //@formatter:off
        return new long[]{
                            0L,  7,  8,  9,              10L, 23, 24, 25,              25L, 14, 15, 16,   1L << 16      , 20, 21, 22,
                1L << 16 |  1L,  4,  5,  6,   1L << 16 | 10L, 17, 18, 19,   1L << 25 | 10L, 11, 12, 13,   1L << 25 | 25L,  1,  2,  3,
                1L << 34      ,  7,  8,  9,   1L << 34 | 10L, 23, 24, 25,   1L << 34 | 25L, 14, 15, 16,   1L << 44      , 20, 21, 22,
                1L << 44 |  1L,  4,  5,  6,   1L << 44 | 10L, 17, 18, 19,   1L << 52 | 10L, 11, 12, 13,   1L << 52 | 25L,  1,  2,  3,
                1L << 57      ,  7,  8,  9,   1L << 57 | 10L, 23, 24, 25,   1L << 57 | 25L, 14, 15, 16,   1L << 62      , 20, 21, 22,
                1L << 62 |  1L,  4,  5,  6,   1L << 62 | 10L, 17, 18, 19,   1L << 63 | 10L, 11, 12, 13,   1L << 63 | 25L,  1,  2,  3,
        };
        //@formatter:on
    }

    private static long[] expectedByTarget() {
        //@formatter:off
        return new long[]{
                 1, 1L << 25 | 25L,  2,  3,    1, 1L << 52 | 25L,  2,  3,    1, 1L << 63 | 25L,  2,  3,    4, 1L << 16 |  1L,  5,  6,
                 4, 1L << 44 |  1L,  5,  6,    4, 1L << 62 |  1L,  5,  6,    7,             0L,  8,  9,    7, 1L << 34      ,  8,  9,
                 7, 1L << 57      ,  8,  9,   11, 1L << 25 | 10L, 12, 13,   11, 1L << 52 | 10L, 12, 13,   11, 1L << 63 | 10L, 12, 13,
                14,            25L, 15, 16,   14, 1L << 34 | 25L, 15, 16,   14, 1L << 57 | 25L, 15, 16,   17, 1L << 16 | 10L, 18, 19,
                17, 1L << 44 | 10L, 18, 19,   17, 1L << 62 | 10L, 18, 19,   20, 1L << 16      , 21, 22,   20, 1L << 44      , 21, 22,
                20, 1L << 62      , 21, 22,   23,            10L, 24, 25,   23, 1L << 34 | 10L, 24, 25,   23, 1L << 57 | 10L, 24, 25,
        };
        //@formatter:on
    }
}
