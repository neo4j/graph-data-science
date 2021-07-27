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
package org.neo4j.graphalgo.similarity.knn;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class NeighborSortTest {

    @Test
    void testSortBySource() {
        long[] data = testData();
        NeighborSort.radixSort(data, NeighborSort.newCopy(data), NeighborSort.newHistogram(0), data.length);
        assertArrayEquals(expectedBySource(), data);
    }

    private static long[] testData() {
        //@formatter:off
        return new long[]{
            1L << 25 | 25L,  1, 1L << 16 |  1L,  4,       0L,  7, 1L << 25 | 10L, 11,
                       25L, 14, 1L << 16 | 10L, 17, 1L << 16, 20,            10L, 23,
            1L << 52 | 25L,  1, 1L << 44 |  1L,  4, 1L << 34,  7, 1L << 52 | 10L, 11,
            1L << 34 | 25L, 14, 1L << 44 | 10L, 17, 1L << 44, 20, 1L << 34 | 10L, 23,
            1L << 63 | 25L,  1, 1L << 62 |  1L,  4, 1L << 57,  7, 1L << 63 | 10L, 11,
            1L << 57 | 25L, 14, 1L << 62 | 10L, 17, 1L << 62, 20, 1L << 57 | 10L, 23,
        };
        //@formatter:on
    }

    private static long[] expectedBySource() {
        //@formatter:off
        return new long[]{
                       0L, 7,            10L, 23,            25L, 14,       1L << 16, 20,
            1L | 1L << 16, 4, 1L << 16 | 10L, 17, 1L << 25 | 10L, 11, 25L | 1L << 25,  1,
                 1L << 34, 7, 1L << 34 | 10L, 23, 1L << 34 | 25L, 14,       1L << 44, 20,
            1L | 1L << 44, 4, 1L << 44 | 10L, 17, 1L << 52 | 10L, 11, 25L | 1L << 52,  1,
                 1L << 57, 7, 1L << 57 | 10L, 23, 1L << 57 | 25L, 14,       1L << 62, 20,
            1L | 1L << 62, 4, 1L << 62 | 10L, 17, 1L << 63 | 10L, 11, 25L | 1L << 63,  1,
        };
        //@formatter:on
    }
}
