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

import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.mem.HugeArrays;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Arrays;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * A long-indexable version of a primitive long array ({@code long[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller long-arrays ({@code long[][]}) to support approx. 32k bn. elements.
 * If the provided size is small enough, an optimized view of a single {@code long[]} might be used.
 *
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse (see {@link org.neo4j.gds.collections.HugeSparseLongArray} for a different implementation that can profit from sparse data).</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code long[]} does ({@code 0}).</li>
 * </ul>
 *
 * <p><em>Basic Usage</em></p>
 * <pre>
 * {@code}
 * AllocationTracker allocationTracker = ...;
 * long arraySize = 42L;
 * HugeLongArray array = HugeLongArray.newArray(arraySize, allocationTracker);
 * array.set(13L, 37L);
 * long value = array.get(13L);
 * // value = 37L
 * {@code}
 * </pre>
 */
public abstract class HugeLongArray extends HugeArray<long[], Long, HugeLongArray> {

    /**
     * @return the long value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract long get(long index);

    /**
     * Sets the long value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, long value);

    /**
     * Computes the bit-wise OR ({@code |}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x | 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void or(long index, final long value);

    /**
     * Computes the bit-wise AND ({@code &}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the 0 ({@code x & 0 == 0}).
     *
     * @return the now current value after the operation
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract long and(long index, final long value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void addTo(long index, long value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link java.util.Arrays#setAll(long[], java.util.function.IntToLongFunction)}.
     */
    public abstract void setAll(LongUnaryOperator gen);

    /**
     * Assigns the specified long value to each element.
     * <p>
     * The behavior is identical to {@link java.util.Arrays#fill(long[], long)}.
     */
    public abstract void fill(long value);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract long size();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract long sizeOf();

    /**
     * Find the index where {@code (values[idx] <= searchValue) && (values[idx + 1] > searchValue)}.
     * The result differs from that of {@link java.util.Arrays#binarySearch(long[], long)}
     * in that this method returns a positive index even if the array does not
     * directly contain the searched value.
     * It returns -1 iff the value is smaller than the smallest one in the array.
     */
    public abstract long binarySearch(long searchValue);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract long release();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract HugeCursor<long[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void copyTo(final HugeLongArray dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    public final HugeLongArray copyOf(final long newLength) {
        HugeLongArray copy = HugeLongArray.newArray(newLength);
        this.copyTo(copy, newLength);
        return copy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final Long boxedGet(final long index) {
        return get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSet(final long index, final Long value) {
        set(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSetAll(final LongFunction<Long> gen) {
        setAll(gen::apply);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedFill(final Long value) {
        fill(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long[] toArray() {
        return dumpToArray(long[].class);
    }

    @Override
    public LongNodePropertyValues asNodeProperties() {
        return new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return get(nodeId);
            }

            @Override
            public long size() {
                return HugeLongArray.this.size();
            }
        };
    }

    /**
     * Creates a new array of the given size.
     */
    public static HugeLongArray newArray(long size) {
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return SingleHugeLongArray.of(size);
        }
        return PagedHugeLongArray.of(size);
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;

        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return MemoryUsage.sizeOfInstance(SingleHugeLongArray.class) + MemoryUsage.sizeOfLongArray((int) size);
        }
        long sizeOfInstance = MemoryUsage.sizeOfInstance(PagedHugeLongArray.class);

        int numPages = HugeArrays.numberOfPages(size);

        long memoryUsed = MemoryUsage.sizeOfObjectArray(numPages);
        final long pageBytes = MemoryUsage.sizeOfLongArray(HugeArrays.PAGE_SIZE);
        memoryUsed += (numPages - 1) * pageBytes;
        final int lastPageSize = HugeArrays.exclusiveIndexOfPage(size);

        return sizeOfInstance + memoryUsed + MemoryUsage.sizeOfLongArray(lastPageSize);
    }

    public static HugeLongArray of(final long... values) {
        return new SingleHugeLongArray(values.length, values);
    }

    public static HugeLongArray of(long[][] array, long size) {
        var capacity = PageUtil.capacityFor(array.length, HugeArrays.PAGE_SHIFT);
        if (size > capacity) {
            throw new IllegalStateException(formatWithLocale("Size should be smaller than or equal to capacity %d, but got size %d", capacity, size));
        }
        return new PagedHugeLongArray(size, array, PagedHugeLongArray.memoryUsed(array, capacity));
    }

    /* test-only */
    static HugeLongArray newPagedArray(long size) {
        return PagedHugeLongArray.of(size);
    }

    /* test-only */
    static HugeLongArray newSingleArray(int size) {
        return SingleHugeLongArray.of(size);
    }

    private static final class SingleHugeLongArray extends HugeLongArray {

        private static HugeLongArray of(long size) {
            assert size <= HugeArrays.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            long[] page = new long[intSize];

            return new SingleHugeLongArray(intSize, page);
        }

        private final int size;
        private long[] page;

        private SingleHugeLongArray(int size, long[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public long get(long index) {
            assert index < size : "index = " + index + " size = " + size;
            return page[(int) index];
        }

        @Override
        public void set(long index, long value) {
            assert index < size;
            page[(int) index] = value;
        }

        @Override
        public void or(long index, final long value) {
            assert index < size;
            page[(int) index] |= value;
        }

        @Override
        public long and(long index, final long value) {
            assert index < size;
            return page[(int) index] &= value;
        }

        @Override
        public void addTo(long index, long value) {
            assert index < size;
            page[(int) index] += value;
        }

        @Override
        public void setAll(LongUnaryOperator gen) {
            Arrays.setAll(page, gen::applyAsLong);
        }

        @Override
        public void fill(long value) {
            Arrays.fill(page, value);
        }

        @Override
        public void copyTo(HugeLongArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeLongArray) {
                SingleHugeLongArray dst = (SingleHugeLongArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, 0L);
            } else if (dest instanceof PagedHugeLongArray) {
                PagedHugeLongArray dst = (PagedHugeLongArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (long[] dstPage : dst.pages) {
                    int toCopy = Math.min(remaining, dstPage.length);
                    if (toCopy == 0) {
                        Arrays.fill(page, 0L);
                    } else {
                        System.arraycopy(page, start, dstPage, 0, toCopy);
                        if (toCopy < dstPage.length) {
                            Arrays.fill(dstPage, toCopy, dstPage.length, 0L);
                        }
                        start += toCopy;
                        remaining -= toCopy;
                    }
                }
            }
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long sizeOf() {
            return MemoryUsage.sizeOfLongArray(size);
        }

        @Override
        public long binarySearch(long searchValue) {
            return ArrayUtil.binaryLookup(searchValue, page);
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return MemoryUsage.sizeOfLongArray(size);
            }
            return 0L;
        }

        @Override
        public HugeCursor<long[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public String toString() {
            return Arrays.toString(page);
        }

        @Override
        public long[] toArray() {
            return page;
        }
    }

    private static final class PagedHugeLongArray extends HugeLongArray {

        private static HugeLongArray of(long size) {
            int numPages = HugeArrays.numberOfPages(size);
            long[][] pages = new long[numPages][];

            for (int i = 0; i < numPages - 1; i++) {
                pages[i] = new long[HugeArrays.PAGE_SIZE];
            }
            int lastPageSize = HugeArrays.exclusiveIndexOfPage(size);
            pages[numPages - 1] = new long[lastPageSize];

            var memoryUsed = memoryUsed(pages, size);

            return new PagedHugeLongArray(size, pages, memoryUsed);
        }

        static long memoryUsed(long[][] pages, long size) {
            var numPages = pages.length;
            long memoryUsed = MemoryUsage.sizeOfObjectArray(numPages);
            long pageBytes = MemoryUsage.sizeOfLongArray(HugeArrays.PAGE_SIZE);
            memoryUsed += pageBytes * (numPages - 1);

            int lastPageSize = HugeArrays.exclusiveIndexOfPage(size);
            memoryUsed += MemoryUsage.sizeOfLongArray(lastPageSize);
            return memoryUsed;
        }

        private final long size;
        private long[][] pages;
        private final long memoryUsed;

        private PagedHugeLongArray(long size, long[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public long get(long index) {
            assert index < size;
            final int pageIndex = HugeArrays.pageIndex(index);
            final int indexInPage = HugeArrays.indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public void set(long index, long value) {
            assert index < size;
            final int pageIndex = HugeArrays.pageIndex(index);
            final int indexInPage = HugeArrays.indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public void or(long index, final long value) {
            assert index < size;
            final int pageIndex = HugeArrays.pageIndex(index);
            final int indexInPage = HugeArrays.indexInPage(index);
            pages[pageIndex][indexInPage] |= value;
        }

        @Override
        public long and(long index, final long value) {
            assert index < size;
            final int pageIndex = HugeArrays.pageIndex(index);
            final int indexInPage = HugeArrays.indexInPage(index);
            return pages[pageIndex][indexInPage] &= value;
        }

        @Override
        public void addTo(long index, long value) {
            assert index < size;
            final int pageIndex = HugeArrays.pageIndex(index);
            final int indexInPage = HugeArrays.indexInPage(index);
            pages[pageIndex][indexInPage] += value;
        }

        @Override
        public void setAll(LongUnaryOperator gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << HugeArrays.PAGE_SHIFT;
                Arrays.setAll(pages[i], j -> gen.applyAsLong(t + j));
            }
        }

        @Override
        public void fill(long value) {
            for (long[] page : pages) {
                Arrays.fill(page, value);
            }
        }

        @Override
        public void copyTo(HugeLongArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeLongArray) {
                SingleHugeLongArray dst = (SingleHugeLongArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (long[] page : pages) {
                    int toCopy = Math.min(remaining, page.length);
                    if (toCopy == 0) {
                        break;
                    }
                    System.arraycopy(page, 0, dst.page, start, toCopy);
                    start += toCopy;
                    remaining -= toCopy;
                }
                Arrays.fill(dst.page, start, dst.size, 0L);
            } else if (dest instanceof PagedHugeLongArray) {
                PagedHugeLongArray dst = (PagedHugeLongArray) dest;
                int pageLen = Math.min(pages.length, dst.pages.length);
                int lastPage = pageLen - 1;
                long remaining = length;
                for (int i = 0; i < lastPage; i++) {
                    long[] page = pages[i];
                    long[] dstPage = dst.pages[i];
                    System.arraycopy(page, 0, dstPage, 0, page.length);
                    remaining -= page.length;
                }
                if (remaining > 0) {
                    System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                    Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, 0L);
                }
                for (int i = pageLen; i < dst.pages.length; i++) {
                    Arrays.fill(dst.pages[i], 0L);
                }
            }
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long sizeOf() {
            return memoryUsed;
        }

        @Override
        public long binarySearch(long searchValue) {
            int value;

            for (int pageIndex = pages.length - 1; pageIndex >= 0; pageIndex--) {
                long[] page = pages[pageIndex];

                value = ArrayUtil.binaryLookup(searchValue, page);
                if (value != -1) {
                    return HugeArrays.indexFromPageIndexAndIndexInPage(pageIndex, value);
                }
            }
            return -1;
        }

        @Override
        public long release() {
            if (pages != null) {
                pages = null;
                return memoryUsed;
            }
            return 0L;
        }

        @Override
        public HugeCursor<long[]> newCursor() {
            return new HugeCursor.PagedCursor<>(size, pages);
        }

    }
}
