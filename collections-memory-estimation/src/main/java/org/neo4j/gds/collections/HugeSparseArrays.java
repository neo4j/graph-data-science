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
import org.neo4j.gds.mem.MemoryUsage;

public final class HugeSparseArrays {

    static final int DEFAULT_PAGE_SHIFT = 12;

    // primitive types

    public static MemoryRange estimateByte(long maxId, long maxEntries) {
        return estimatePrimitive(byte.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateByte(long maxId, long maxEntries, int pageShift) {
        return estimatePrimitive(byte.class, maxId, maxEntries, pageShift);
    }

    public static MemoryRange estimateShort(long maxId, long maxEntries) {
        return estimatePrimitive(short.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateShort(long maxId, long maxEntries, int pageShift) {
        return estimatePrimitive(short.class, maxId, maxEntries, pageShift);
    }

    public static MemoryRange estimateInt(long maxId, long maxEntries) {
        return estimatePrimitive(int.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateInt(long maxId, long maxEntries, int pageShift) {
        return estimatePrimitive(int.class, maxId, maxEntries, pageShift);
    }

    public static MemoryRange estimateLong(long maxId, long maxEntries) {
        return estimatePrimitive(long.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateLong(long maxId, long maxEntries, int pageShift) {
        return estimatePrimitive(long.class, maxId, maxEntries, pageShift);
    }

    public static MemoryRange estimateFloat(long maxId, long maxEntries) {
        return estimatePrimitive(float.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateFloat(long maxId, long maxEntries, int pageShift) {
        return estimatePrimitive(float.class, maxId, maxEntries, pageShift);
    }

    public static MemoryRange estimateDouble(long maxId, long maxEntries) {
        return estimatePrimitive(double.class, maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateDouble(long maxId, long maxEntries, int pageShift) {
        return estimatePrimitive(double.class, maxId, maxEntries, pageShift);
    }

    // array types

    public static MemoryRange estimateArray(long maxId, long maxEntries) {
        return estimateArray(maxId, maxEntries, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateArray(long maxId, long maxEntries, int pageShift) {
        // If the value array length is not known upfront, we estimate assuming an empty array.
        // Therefore, we can use any array type here since we only measure object references.
        return estimateArray(long[].class, maxId, maxEntries, 0, pageShift);
    }

    public static MemoryRange estimateByteArray(long maxId, long maxEntries, int averageEntryLength) {
        return estimateArray(byte[].class, maxId, maxEntries, averageEntryLength, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateByteArray(long maxId, long maxEntries, int averageEntryLength, int pageShift) {
        return estimateArray(byte[].class, maxId, maxEntries, averageEntryLength, pageShift);
    }

    public static MemoryRange estimateShortArray(long maxId, long maxEntries, int averageEntryLength) {
        return estimateArray(short[].class, maxId, maxEntries, averageEntryLength, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateShortArray(long maxId, long maxEntries, int averageEntryLength, int pageShift) {
        return estimateArray(short[].class, maxId, maxEntries, averageEntryLength, pageShift);
    }

    public static MemoryRange estimateIntArray(long maxId, long maxEntries, int averageEntryLength) {
        return estimateArray(int[].class, maxId, maxEntries, averageEntryLength, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateIntArray(long maxId, long maxEntries, int averageEntryLength, int pageShift) {
        return estimateArray(int[].class, maxId, maxEntries, averageEntryLength, pageShift);
    }

    public static MemoryRange estimateLongArray(long maxId, long maxEntries, int averageEntryLength) {
        return estimateArray(long[].class, maxId, maxEntries, averageEntryLength, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateLongArray(long maxId, long maxEntries, int averageEntryLength, int pageShift) {
        return estimateArray(long[].class, maxId, maxEntries, averageEntryLength, pageShift);
    }

    public static MemoryRange estimateFloatArray(long maxId, long maxEntries, int averageEntryLength) {
        return estimateArray(float[].class, maxId, maxEntries, averageEntryLength, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateFloatArray(long maxId, long maxEntries, int averageEntryLength, int pageShift) {
        return estimateArray(float[].class, maxId, maxEntries, averageEntryLength, pageShift);
    }

    public static MemoryRange estimateDoubleArray(long maxId, long maxEntries, int averageEntryLength) {
        return estimateArray(double[].class, maxId, maxEntries, averageEntryLength, DEFAULT_PAGE_SHIFT);
    }

    public static MemoryRange estimateDoubleArray(long maxId, long maxEntries, int averageEntryLength, int pageShift) {
        return estimateArray(double[].class, maxId, maxEntries, averageEntryLength, pageShift);
    }

    static MemoryRange estimatePrimitive(Class<?> valueClazz, long maxId, long maxEntries, int pageShift) {
        var collectionAndPageSize = CollectionAndElementSize.ofPrimitive(valueClazz);
        return memoryEstimationPrimitive(
            collectionAndPageSize,
            maxId,
            maxEntries,
            pageShift
        );
    }

    static MemoryRange estimateArray(
        Class<?> valueClazz,
        long maxId,
        long maxEntries,
        int averageEntryLength,
        int pageShift
    ) {
        var collectionAndPageSize = CollectionAndElementSize.ofArray(valueClazz, averageEntryLength);
        return memoryEstimationArray(
            collectionAndPageSize,
            maxId,
            maxEntries,
            pageShift
        );
    }

    private static MemoryRange memoryEstimationPrimitive(
        CollectionAndElementSize collectionAndElementSize,
        long maxIndex,
        long expectedNumValues,
        int pageShift
    ) {
        assert (expectedNumValues <= maxIndex);

        int pageSize = 1 << pageShift;
        int pageMask = pageSize - 1;

        int numPagesForSize = PageUtil.numPagesFor(maxIndex, pageShift, pageMask);
        int numPagesForExpectedNumValuesBestCase = PageUtil.numPagesFor(expectedNumValues, pageShift, pageMask);

        // Worst-case distribution assumes at least one entry per page
        long maxEntriesForWorstCase = Math.min(maxIndex, expectedNumValues * pageSize);
        int numPagesForExpectedNumValuesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, pageShift, pageMask);

        long classSize = MemoryUsage.sizeOfInstance(collectionAndElementSize.collectionClazz);
        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);

        long minRequirements = numPagesForExpectedNumValuesBestCase * MemoryUsage.sizeOfArray(
            pageSize,
            collectionAndElementSize.elementSizeInBytes
        );
        long maxRequirements = numPagesForExpectedNumValuesWorstCase * MemoryUsage.sizeOfArray(
            pageSize,
            collectionAndElementSize.elementSizeInBytes
        );

        return MemoryRange.of(classSize + pagesSize).add(MemoryRange.of(minRequirements, maxRequirements));
    }

    private static MemoryRange memoryEstimationArray(
        CollectionAndElementSize collectionAndElementSize,
        long maxIndex,
        long expectedNumValues,
        int pageShift
    ) {
        assert (expectedNumValues <= maxIndex);

        int pageSize = 1 << pageShift;
        int pageMask = pageSize - 1;

        int numPagesForSize = PageUtil.numPagesFor(maxIndex, pageShift, pageMask);
        int numPagesForExpectedNumValuesBestCase = PageUtil.numPagesFor(expectedNumValues, pageShift, pageMask);

        // Worst-case distribution assumes at least one entry per page
        long maxEntriesForWorstCase = Math.min(maxIndex, expectedNumValues * pageSize);
        int numPagesForExpectedNumValuesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, pageShift, pageMask);

        long classSize = MemoryUsage.sizeOfInstance(collectionAndElementSize.collectionClazz);
        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);

        var pagesSizeBestCase = numPagesForExpectedNumValuesBestCase * MemoryUsage.sizeOfObjectArray(pageSize);
        var pagesSizeWorstCase = numPagesForExpectedNumValuesWorstCase * MemoryUsage.sizeOfObjectArray(pageSize);

        long valuesSize = expectedNumValues * collectionAndElementSize.elementSizeInBytes;

        return MemoryRange
            .of(classSize + pagesSize)
            .add(MemoryRange.of(valuesSize))
            .add(MemoryRange.of(pagesSizeBestCase, pagesSizeWorstCase));
    }

    private static final class CollectionAndElementSize {
        Class<?> collectionClazz;

        long elementSizeInBytes;

        private CollectionAndElementSize(Class<?> collectionClazz, long elementSizeInBytes) {
            this.collectionClazz = collectionClazz;
            this.elementSizeInBytes = elementSizeInBytes;
        }

        static CollectionAndElementSize ofPrimitive(Class<?> valueClazz) {
            if (valueClazz.isAssignableFrom(byte.class)) {
                return new CollectionAndElementSize(HugeSparseByteArraySon.class, Byte.BYTES);
            }
            if (valueClazz.isAssignableFrom(short.class)) {
                return new CollectionAndElementSize(HugeSparseShortArraySon.class, Short.BYTES);
            }
            if (valueClazz.isAssignableFrom(int.class)) {
                return new CollectionAndElementSize(HugeSparseIntArraySon.class, Integer.BYTES);
            }
            if (valueClazz.isAssignableFrom(long.class)) {
                return new CollectionAndElementSize(HugeSparseLongArrayFoo.class, Long.BYTES);
            }
            if (valueClazz.isAssignableFrom(float.class)) {
                return new CollectionAndElementSize(HugeSparseFloatArraySon.class, Float.BYTES);
            }
            if (valueClazz.isAssignableFrom(double.class)) {
                return new CollectionAndElementSize(HugeSparseDoubleArraySon.class, Double.BYTES);
            }
            throw new IllegalArgumentException("Unsupported primitive value class: " + valueClazz);
        }

        static CollectionAndElementSize ofArray(Class<?> valueClazz, int averageEntryLength) {
            if (valueClazz.isAssignableFrom(byte[].class)) {
                return new CollectionAndElementSize(
                    HugeSparseByteArrayArraySon.class,
                    MemoryUsage.sizeOfArray(averageEntryLength, Byte.BYTES)
                );
            }
            if (valueClazz.isAssignableFrom(short[].class)) {
                return new CollectionAndElementSize(
                    HugeSparseShortArrayArraySon.class,
                    MemoryUsage.sizeOfArray(averageEntryLength, Short.BYTES)
                );
            }
            if (valueClazz.isAssignableFrom(int[].class)) {
                return new CollectionAndElementSize(
                    HugeSparseIntArrayArraySon.class,
                    MemoryUsage.sizeOfArray(averageEntryLength, Integer.BYTES)
                );
            }
            if (valueClazz.isAssignableFrom(long[].class)) {
                return new CollectionAndElementSize(
                    HugeSparseLongArrayArraySon.class,
                    MemoryUsage.sizeOfArray(averageEntryLength, Long.BYTES)
                );
            }
            if (valueClazz.isAssignableFrom(float[].class)) {
                return new CollectionAndElementSize(
                    HugeSparseFloatArrayArraySon.class,
                    MemoryUsage.sizeOfArray(averageEntryLength, Float.BYTES)
                );
            }
            if (valueClazz.isAssignableFrom(double[].class)) {
                return new CollectionAndElementSize(
                    HugeSparseDoubleArrayArraySon.class,
                    MemoryUsage.sizeOfArray(averageEntryLength, Double.BYTES)
                );
            }
            throw new IllegalArgumentException("Unsupported primitive value class: " + valueClazz);
        }
    }

    private HugeSparseArrays() {}
}
