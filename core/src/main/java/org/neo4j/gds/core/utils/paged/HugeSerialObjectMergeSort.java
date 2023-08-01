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

import org.neo4j.gds.collections.ha.HugeObjectArray;

import java.util.function.ToDoubleFunction;


public final class HugeSerialObjectMergeSort {
    private HugeSerialObjectMergeSort() {}

    public static <T> void sort(Class<T> componentClass, HugeObjectArray<T> array, ToDoubleFunction<T> toSortValue) {
        HugeObjectArray<T> temp = HugeObjectArray.newArray(componentClass, array.size());
        sort(array, array.size(), toSortValue, temp);
    }

    public static <T> void sort(
        HugeObjectArray<T> array,
        long size,
        ToDoubleFunction<T> toSortValue,
        HugeObjectArray<T> temp
    ) {
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

    private static <T> void merge(
        HugeObjectArray<T> array,
        HugeObjectArray<T> temp,
        ToDoubleFunction<T> toSortValue,
        long leftStart,
        long leftEnd,
        long rightStart,
        long rightEnd
    ) {
        long idx = 0;

        while (leftStart <= leftEnd && rightStart <= rightEnd) {
            T lsIdx = array.get(leftStart);
            T rsIdx = array.get(rightStart);
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
