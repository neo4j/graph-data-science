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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TwoArraysQuickSortTest {

    @Test
    void sortDoubleArrayByLongValues() {
        long[] longData = new long[]{10, 9, 8, 7, 6, 5};
        double[] doubleData = new double[]{1, 2, 3, 4, 5, 6};

        TwoArraysQuickSort.sortDoubleArrayByLongValues(longData, doubleData, longData.length);
        for (int i = 0; i < longData.length - 1; i++) {
            assertTrue(longData[i] < longData[i + 1]);
            assertTrue(doubleData[i] > doubleData[i + 1]);
        }
    }

    @Test
    void sortDoubleArrayByLongValuesAsc() {
        long[] longData = new long[]{0, 5, 3, 8, 10, 12};
        double[] doubleData = new double[]{0, 2, 1, 3, 4, 5};

        TwoArraysQuickSort.sortDoubleArrayByLongValues(longData, doubleData, longData.length);
        for (int i = 0; i < longData.length - 1; i++) {
            assertTrue(longData[i] < longData[i + 1]);
            assertTrue(doubleData[i] < doubleData[i + 1]);
        }
    }

    @Test
    void sortDoubleArrayByLongValuesShort() {
        long[] longData = new long[]{1, 0};
        double[] doubleData = new double[]{10.0, 0.0};

        TwoArraysQuickSort.sortDoubleArrayByLongValues(longData, doubleData, longData.length);
        for (int i = 0; i < longData.length - 1; i++) {
            assertTrue(longData[i] < longData[i + 1]);
            assertTrue(doubleData[i] < doubleData[i + 1]);
        }
    }

    @Test
    void sortDoubleArrayByLongValuesOne() {
        long[] longData = new long[]{1};
        double[] doubleData = new double[]{10.0};

        TwoArraysQuickSort.sortDoubleArrayByLongValues(longData, doubleData, longData.length);
        assertEquals(1, longData[0]);
        assertEquals(10.0, doubleData[0]);
    }

    @Test
    void sortDoubleArrayByLongValuesNotFull() {
        long[] longData = new long[]{2, 1, 3, 0, 0, 0};
        double[] doubleData = new double[]{20.0, 10.0, 30.0, 0.0, 0.0, 0.0};

        var targetLength = 3;
        TwoArraysQuickSort.sortDoubleArrayByLongValues(longData, doubleData, targetLength);
        for (int i = 0; i < targetLength - 1; i++) {
            assertTrue(longData[i] < longData[i + 1]);
            assertTrue(doubleData[i] < doubleData[i + 1]);
        }
        assertEquals(0, longData[3]);
        assertEquals(0, longData[4]);
        assertEquals(0, longData[5]);
        assertEquals(0.0, doubleData[3]);
        assertEquals(0.0, doubleData[4]);
        assertEquals(0.0, doubleData[5]);
    }
}
