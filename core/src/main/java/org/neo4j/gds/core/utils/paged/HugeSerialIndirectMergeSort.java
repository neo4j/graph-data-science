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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.function.LongToDoubleFunction;


// Only runs single-threaded. See HugeMergeSort for a concurrent version.
public final class HugeSerialIndirectMergeSort {
    private HugeSerialIndirectMergeSort() {}

    public static void sort(HugeLongArray array, LongToDoubleFunction toSortValue) {
        HugeLongArray temp = HugeLongArray.newArray(array.size());
        sort(array, array.size(), toSortValue, temp);
    }
    public static void sort(HugeLongArray array, long size, LongToDoubleFunction toSortValue, HugeLongArray temp) {
        long tempSize = 1;

        while (tempSize < size) {
            long i = 0;

            while (i < size) {
                long leftStart = i;
                long leftEnd = i + tempSize - 1;
                long rightStart = i + tempSize;
                long rightEnd = i + 2 * tempSize - 1;

                if (rightStart >= size) {
                    break;
                }

                if (rightEnd >= size) {
                    rightEnd = size - 1;
                }

                merge(array, temp, toSortValue, leftStart, leftEnd, rightStart, rightEnd);

                for (long j = 0; j < rightEnd - leftStart + 1; j++) {
                    array.set(i + j, temp.get(j));
                }

                i = i + 2 * tempSize;
            }
            tempSize *= 2;
        }
    }

    private static void merge(
        HugeLongArray array,
        HugeLongArray temp,
        LongToDoubleFunction toSortValue,
        long leftStart,
        long leftEnd,
        long rightStart,
        long rightEnd
    ) {
        long idx = 0;

        while (leftStart <= leftEnd && rightStart <= rightEnd) {
            long lsIdx = array.get(leftStart);
            long rsIdx = array.get(rightStart);
            if (Double.compare(toSortValue.applyAsDouble(lsIdx), toSortValue.applyAsDouble(rsIdx)) <= 0) {
                temp.set(idx++, lsIdx);
                leftStart++;
            } else {
                temp.set(idx++, rsIdx);
                rightStart++;
            }
        }

        while (leftStart <= leftEnd) {
            temp.set(idx++, array.get(leftStart++));
        }

        while (rightStart <= rightEnd) {
            temp.set(idx++, array.get(rightStart++));
        }
    }
}
