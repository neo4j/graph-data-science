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

import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.mem.HugeArrays;

import java.util.Arrays;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;

import static org.neo4j.gds.mem.HugeArrays.PAGE_SHIFT;
import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.gds.mem.HugeArrays.indexInPage;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.HugeArrays.pageIndex;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;

/**
 * A long-indexable version of a primitive int array ({@code int[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller int-arrays ({@code int[][]}) to support approx. 32k bn. elements.
 * If the provided size is small enough, an optimized view of a single {@code int[]} might be used.
 *
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse (see {@link org.neo4j.gds.collections.HugeSparseIntArray} for a different implementation that can profit from sparse data).</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code int[]} does ({@code 0}).</li>
 * </ul>
 *
 * <p><em>Basic Usage</em></p>
 * <pre>
 * {@code}
 * AllocationTracker allocationTracker = ...;
 * long arraySize = 42L;
 * HugeIntArray array = HugeIntArray.newArray(arraySize, allocationTracker);
 * array.set(13L, 37);
 * int value = array.get(13L);
 * // value = 37
 * {@code}
 * </pre>
 */
public abstract class HugeIntArray extends HugeArray<int[], Integer, HugeIntArray> {

    /**
     * @return the int value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract int get(long index);

    public abstract int getAndAdd(long index, int delta);

    /**
     * Sets the int value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, int value);

    /**
     * Computes the bit-wise OR ({@code |}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x | 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void or(long index, int value);

    /**
     * Computes the bit-wise AND ({@code &}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the 0 ({@code x & 0 == 0}).
     *
     * @return the now current value after the operation
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract int and(long index, int value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void addTo(long index, int value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(int[], java.util.function.IntUnaryOperator)}.
     */
    public abstract void setAll(LongToIntFunction gen);

    /**
     * Assigns the specified int value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(int[], int)}.
     */
    public abstract void fill(int value);

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
     * {@inheritDoc}
     */
    @Override
    public abstract long release();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract HugeCursor<int[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void copyTo(final HugeIntArray dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    public final HugeIntArray copyOf(final long newLength) {
        HugeIntArray copy = HugeIntArray.newArray(newLength);
        this.copyTo(copy, newLength);
        return copy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final Integer boxedGet(final long index) {
        return get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSet(final long index, final Integer value) {
        set(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSetAll(final LongFunction<Integer> gen) {
        setAll(gen::apply);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedFill(final Integer value) {
        fill(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] toArray() {
        return dumpToArray(int[].class);
    }

    @Override
    public LongNodePropertyValues asNodeProperties() {
        return new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return get(nodeId);
            }

            @Override
            public long nodeCount() {
                return HugeIntArray.this.size();
            }
        };
    }

    /**
     * Creates a new array of the given size.
     */
    public static HugeIntArray newArray(long size) {
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return SingleHugeIntArray.of(size);
        }
        return PagedHugeIntArray.of(size);
    }

    public static HugeIntArray of(final int... values) {
        return new HugeIntArray.SingleHugeIntArray(values.length, values);
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;

        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return sizeOfInstance(SingleHugeIntArray.class) + sizeOfIntArray((int) size);
        }
        long sizeOfInstance = sizeOfInstance(PagedHugeIntArray.class);

        int numPages = numberOfPages(size);

        long memoryUsed = sizeOfObjectArray(numPages);
        long pageBytes = sizeOfIntArray(PAGE_SIZE);
        memoryUsed += (numPages - 1) * pageBytes;
        int lastPageSize = exclusiveIndexOfPage(size);

        return sizeOfInstance + memoryUsed + sizeOfIntArray(lastPageSize);
    }

    /* test-only */
    static HugeIntArray newPagedArray(long size) {
        return PagedHugeIntArray.of(size);
    }

    /* test-only */
    static HugeIntArray newSingleArray(int size) {
        return SingleHugeIntArray.of(size);
    }

    private static final class SingleHugeIntArray extends HugeIntArray {

        private static HugeIntArray of(long size) {
            assert size <= HugeArrays.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            int[] page = new int[intSize];

            return new SingleHugeIntArray(intSize, page);
        }

        private final int size;
        private int[] page;

        private SingleHugeIntArray(int size, int[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public int get(long index) {
            assert index < size;
            return page[(int) index];
        }

        @Override
        public int getAndAdd(long index, int delta) {
            assert index < size;
            var idx = (int) index;
            var value = page[idx];
            page[idx] += delta;
            return value;
        }

        @Override
        public void set(long index, int value) {
            assert index < size;
            page[(int) index] = value;
        }

        @Override
        public void or(long index, int value) {
            assert index < size;
            page[(int) index] |= value;
        }

        @Override
        public int and(long index, int value) {
            assert index < size;
            return page[(int) index] &= value;
        }

        @Override
        public void addTo(long index, int value) {
            assert index < size;
            page[(int) index] += value;
        }

        @Override
        public void setAll(LongToIntFunction gen) {
            Arrays.setAll(page, gen::applyAsInt);
        }

        @Override
        public void fill(int value) {
            Arrays.fill(page, value);
        }

        @Override
        public void copyTo(HugeIntArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeIntArray) {
                SingleHugeIntArray dst = (SingleHugeIntArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, 0);
            } else if (dest instanceof PagedHugeIntArray) {
                PagedHugeIntArray dst = (PagedHugeIntArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (int[] dstPage : dst.pages) {
                    int toCopy = Math.min(remaining, dstPage.length);
                    if (toCopy == 0) {
                        Arrays.fill(page, 0);
                    } else {
                        System.arraycopy(page, start, dstPage, 0, toCopy);
                        if (toCopy < dstPage.length) {
                            Arrays.fill(dstPage, toCopy, dstPage.length, 0);
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
            return sizeOfIntArray(size);
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return sizeOfIntArray(size);
            }
            return 0L;
        }

        @Override
        public HugeCursor<int[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public int[] toArray() {
            return page;
        }

        @Override
        public String toString() {
            return Arrays.toString(page);
        }
    }

    private static final class PagedHugeIntArray extends HugeIntArray {

        private static HugeIntArray of(long size) {
            int numPages = numberOfPages(size);
            int[][] pages = new int[numPages][];

            long memoryUsed = sizeOfObjectArray(numPages);
            final long pageBytes = sizeOfIntArray(PAGE_SIZE);
            for (int i = 0; i < numPages - 1; i++) {
                memoryUsed += pageBytes;
                pages[i] = new int[PAGE_SIZE];
            }
            final int lastPageSize = exclusiveIndexOfPage(size);
            pages[numPages - 1] = new int[lastPageSize];
            memoryUsed += sizeOfIntArray(lastPageSize);

            return new PagedHugeIntArray(size, pages, memoryUsed);
        }

        private final long size;
        private int[][] pages;
        private final long memoryUsed;

        private PagedHugeIntArray(long size, int[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public int get(long index) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public int getAndAdd(long index, int delta) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            var value = pages[pageIndex][indexInPage];
            pages[pageIndex][indexInPage] += delta;
            return value;
        }

        @Override
        public void set(long index, int value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public void or(long index, int value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] |= value;
        }

        @Override
        public int and(long index, int value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage] &= value;
        }

        @Override
        public void addTo(long index, int value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] += value;
        }

        @Override
        public void setAll(LongToIntFunction gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << PAGE_SHIFT;
                Arrays.setAll(pages[i], j -> gen.applyAsInt(t + j));
            }
        }

        @Override
        public void fill(int value) {
            for (int[] page : pages) {
                Arrays.fill(page, value);
            }
        }

        @Override
        public void copyTo(HugeIntArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeIntArray) {
                SingleHugeIntArray dst = (SingleHugeIntArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (int[] page : pages) {
                    int toCopy = Math.min(remaining, page.length);
                    if (toCopy == 0) {
                        break;
                    }
                    System.arraycopy(page, 0, dst.page, start, toCopy);
                    start += toCopy;
                    remaining -= toCopy;
                }
                Arrays.fill(dst.page, start, dst.size, 0);
            } else if (dest instanceof PagedHugeIntArray) {
                PagedHugeIntArray dst = (PagedHugeIntArray) dest;
                int pageLen = Math.min(pages.length, dst.pages.length);
                int lastPage = pageLen - 1;
                long remaining = length;
                for (int i = 0; i < lastPage; i++) {
                    int[] page = pages[i];
                    int[] dstPage = dst.pages[i];
                    System.arraycopy(page, 0, dstPage, 0, page.length);
                    remaining -= page.length;
                }
                if (remaining > 0L) {
                    System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                    Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, 0);
                }
                for (int i = pageLen; i < dst.pages.length; i++) {
                    Arrays.fill(dst.pages[i], 0);
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
        public long release() {
            if (pages != null) {
                pages = null;
                return memoryUsed;
            }
            return 0L;
        }

        @Override
        public HugeCursor<int[]> newCursor() {
            return new HugeCursor.PagedCursor<>(size, pages);
        }
    }
}
