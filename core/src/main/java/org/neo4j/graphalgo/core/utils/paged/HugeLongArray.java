/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Arrays;
import java.util.function.IntToLongFunction;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.SINGLE_PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.numberOfPages;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

/**
 * A long-indexable version of a primitive long array ({@code long[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller long-arrays ({@code long[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code long[]} might be used.
 * <p>
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse (see {@link org.neo4j.graphalgo.core.huge.loader.SparseNodeMapping} for a different implementation that can profit from sparse data).</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code long[]} does ({@code 0}).</li>
 * </ul>
 * <p>
 * <h3>Basic Usage</h3>
 * <pre>
 * {@code}
 * AllocationTracker tracker = ...;
 * long arraySize = 42L;
 * HugeLongArray array = HugeLongArray.newArray(arraySize, tracker);
 * array.set(13L, 37L);
 * long value = array.get(13L);
 * // value = 37L
 * {@code}
 * </pre>
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class HugeLongArray extends HugeArray<long[], Long, HugeLongArray> {



    /**
     * @return the long value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public long get(long index);

    /**
     * Sets the long value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void set(long index, long value);

    /**
     * Computes the bit-wise OR ({@code |}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x | 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void or(long index, final long value);

    /**
     * Computes the bit-wise AND ({@code &}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the 0 ({@code x & 0 == 0}).
     *
     * @return the now current value after the operation
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public long and(long index, final long value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void addTo(long index, long value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(long[], IntToLongFunction)}.
     */
    abstract public void setAll(LongUnaryOperator gen);

    /**
     * Assigns the specified long value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(long[], long)}.
     */
    abstract public void fill(long value);

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public long size();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public long sizeOf();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public long release();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public HugeCursor<long[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public void copyTo(final HugeLongArray dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    public final HugeLongArray copyOf(final long newLength, final AllocationTracker tracker) {
        HugeLongArray copy = HugeLongArray.newArray(newLength, tracker);
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

    /**
     * Creates a new array if the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static HugeLongArray newArray(long size, AllocationTracker tracker) {
        if (size <= SINGLE_PAGE_SIZE) {
            return SingleHugeLongArray.of(size, tracker);
        }
        return PagedHugeLongArray.of(size, tracker);
    }

    public static long memoryRequirements(long size) {
        assert size >= 0;

        if (size <= SINGLE_PAGE_SIZE) {
            return sizeOfInstance(SingleHugeLongArray.class) + sizeOfLongArray((int)size);
        }
        long sizeOfInstance = sizeOfInstance(PagedHugeLongArray.class);

        int numPages = numberOfPages(size);

        long memoryUsed = sizeOfObjectArray(numPages);
        final long pageBytes = sizeOfLongArray(PAGE_SIZE);
        memoryUsed += (numPages - 1) * pageBytes;
        final int lastPageSize = exclusiveIndexOfPage(size);

        return sizeOfInstance + memoryUsed + sizeOfLongArray(lastPageSize);
    }

    public static HugeLongArray of(final long... values) {
        return new SingleHugeLongArray(values.length, values);
    }

    /* test-only */
    static HugeLongArray newPagedArray(long size, AllocationTracker tracker) {
        return PagedHugeLongArray.of(size, tracker);
    }

    /* test-only */
    static HugeLongArray newSingleArray(int size, AllocationTracker tracker) {
        return SingleHugeLongArray.of(size, tracker);
    }

    /**
     * A {@link PropertyTranslator} for instances of {@link HugeLongArray}s.
     */
    public static class Translator implements PropertyTranslator.OfLong<HugeLongArray> {

        public static final Translator INSTANCE = new Translator();

        @Override
        public long toLong(final HugeLongArray data, final long nodeId) {
            return data.get(nodeId);
        }
    }

    private static final class SingleHugeLongArray extends HugeLongArray {

        private static HugeLongArray of(long size, AllocationTracker tracker) {
            assert size <= SINGLE_PAGE_SIZE;
            final int intSize = (int) size;
            long[] page = new long[intSize];
            tracker.add(sizeOfLongArray(intSize));

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
            assert index < size;
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
            return sizeOfLongArray(size);
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return sizeOfLongArray(size);
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

        private static HugeLongArray of(long size, AllocationTracker tracker) {
            int numPages = numberOfPages(size);
            long[][] pages = new long[numPages][];

            long memoryUsed = sizeOfObjectArray(numPages);
            final long pageBytes = sizeOfLongArray(PAGE_SIZE);
            for (int i = 0; i < numPages - 1; i++) {
                memoryUsed += pageBytes;
                pages[i] = new long[PAGE_SIZE];
            }
            final int lastPageSize = exclusiveIndexOfPage(size);
            pages[numPages - 1] = new long[lastPageSize];
            memoryUsed += sizeOfLongArray(lastPageSize);
            tracker.add(memoryUsed);

            return new PagedHugeLongArray(size, pages, memoryUsed);
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
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public void set(long index, long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public void or(long index, final long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] |= value;
        }

        @Override
        public long and(long index, final long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage] &= value;
        }

        @Override
        public void addTo(long index, long value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] += value;
        }

        @Override
        public void setAll(LongUnaryOperator gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << PAGE_SHIFT;
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
