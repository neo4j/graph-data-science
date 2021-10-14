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
package org.neo4j.gds.collections;

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.PageUtil;
import org.neo4j.gds.mem.MemoryUsage;

public final class HugeSparseArrays {

    static final int DEFAULT_PAGE_SHIFT = 12;

    public static MemoryRange estimateLong(long maxId, long maxEntries) {
        return estimate(long.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateLong(long maxId, long maxEntries, int pageShift) {
        return estimate(long.class, maxId, maxEntries, pageShift);
    }

    public static MemoryRange estimateLongArray(long maxId, long maxEntries) {
        return estimate(long[].class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateLongArray(long maxId, long maxEntries, int pageShift) {
        return estimate(long[].class, maxId, maxEntries, pageShift);
    }

    static MemoryRange estimate(Class<?> valueClazz, long maxId, long maxEntries, int pageShift) {
        var collectionClazz = collectionClazz(valueClazz);
        var pageSizeInBytes = pageSizeInBytes(valueClazz, pageSize(pageShift));
        return memoryEstimation(collectionClazz, maxId, maxEntries, pageShift, pageSizeInBytes);
    }

    private static MemoryRange memoryEstimation(
        Class<?> clazz,
        long maxIndex,
        long expectedNumValues,
        int pageShift,
        long pageSizeInBytes
    ) {
        assert (expectedNumValues <= maxIndex);

        int pageSize = 1 << pageShift;
        int pageMask = pageSize - 1;

        int numPagesForSize = PageUtil.numPagesFor(maxIndex, pageShift, pageMask);
        int numPagesForExpectedNumValuesBestCase = PageUtil.numPagesFor(expectedNumValues, pageShift, pageMask);

        // Worst-case distribution assumes at least one entry per page
        long maxEntriesForWorstCase = Math.min(maxIndex, expectedNumValues * pageSize);
        int numPagesForExpectedNumValuesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, pageShift, pageMask);

        long classSize = MemoryUsage.sizeOfInstance(clazz);
        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);
        long minRequirements = numPagesForExpectedNumValuesBestCase * pageSizeInBytes;
        long maxRequirements = numPagesForExpectedNumValuesWorstCase * pageSizeInBytes;

        return MemoryRange.of(classSize + pagesSize).add(MemoryRange.of(minRequirements, maxRequirements));
    }

    private static int pageSize(int pageShift) {
        return 1 << pageShift;
    }

    private static long pageSizeInBytes(Class<?> valueClazz, int pageSize) {
        if (valueClazz.isAssignableFrom(long.class)) {
            return MemoryUsage.sizeOfLongArray(pageSize);
        }
        if (valueClazz.isAssignableFrom(long[].class)) {
            return MemoryUsage.sizeOfObjectArray(pageSize);
        }
        throw new IllegalArgumentException("Unsupported value class: " + valueClazz);
    }

    private static Class<?> collectionClazz(Class<?> valueClazz) {
        if (valueClazz.isAssignableFrom(long.class)) {
            return HugeSparseLongArraySon.class;
        }
        if (valueClazz.isAssignableFrom(long[].class)) {
            return HugeSparseLongArrayArraySon.class;
        }
        throw new IllegalArgumentException("Unsupported value class: " + valueClazz);
    }


    private HugeSparseArrays() {}
}
