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
package org.neo4j.graphalgo.core.utils.paged;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.SINGLE_PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.numberOfPages;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;

/**
 * A long-indexable version of a Object array ({@code T[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller object-arrays ({@code T[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code T[]} might be used.
 * <p>
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code T[]} does ({@code null}).</li>
 * </ul>
 * <p>
 * <h3>Basic Usage</h3>
 * <pre>
 * {@code}
 * AllocationTracker tracker = ...;
 * long arraySize = 42L;
 * HugeObjectArray&lt;String&gt; array = HugeObjectArray.newArray(String.class, arraySize, tracker);
 * array.set(13L, "37");
 * String value = array.get(13L);
 * // value = "37"
 * {@code}
 * </pre>
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class HugeObjectArray<T> extends HugeArray<T[], T, HugeObjectArray<T>> {

    /**
     * @return the value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public T get(long index);

    /**
     * Sets the value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void set(long index, T value);

    /**
     * If the value at the given index is {@code null}, attempts to compute its value using
     * the given supplier and enters it into this array unless {@code null}.
     *
     * @param index index at which the specified value is to be associated
     * @return the current (existing or computed) value associated with
     *         the specified index, or null if the computed value is null
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public T putIfAbsent(long index, Supplier<T> supplier);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(Object[], IntFunction)}.
     */
    abstract public void setAll(LongFunction<T> gen);

    /**
     * Assigns the specified value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(Object[], Object)}.
     */
    abstract public void fill(T value);

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
    abstract public HugeCursor<T[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public void copyTo(final HugeObjectArray dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public HugeObjectArray<T> copyOf(final long newLength, final AllocationTracker tracker);

    /**
     * {@inheritDoc}
     */
    @Override
    final T boxedGet(final long index) {
        return get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSet(final long index, final T value) {
        set(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSetAll(final LongFunction<T> gen) {
        setAll(gen);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedFill(final T value) {
        fill(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public T[] toArray();

    /**
     * Creates a new array if the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static <T> HugeObjectArray<T> newArray(Class<T> componentClass, long size, AllocationTracker tracker) {
        if (size <= SINGLE_PAGE_SIZE) {
            return SingleHugeObjectArray.of(componentClass, size, tracker);
        }
        return PagedHugeObjectArray.of(componentClass, size, tracker);
    }

    @SafeVarargs
    public static <T> HugeObjectArray of(final T... values) {
        return new HugeObjectArray.SingleHugeObjectArray<>(values.length, values);
    }

    /* test-only */
    static <T> HugeObjectArray<T> newPagedArray(Class<T> componentClass, long size, AllocationTracker tracker) {
        return PagedHugeObjectArray.of(componentClass, size, tracker);
    }

    /* test-only */
    static <T> HugeObjectArray<T> newSingleArray(Class<T> componentClass, int size, AllocationTracker tracker) {
        return SingleHugeObjectArray.of(componentClass, size, tracker);
    }

    private static final class SingleHugeObjectArray<T> extends HugeObjectArray<T> {

        private static <T> HugeObjectArray<T> of(Class<T> componentClass, long size, AllocationTracker tracker) {
            assert size <= SINGLE_PAGE_SIZE;
            final int intSize = (int) size;
            //noinspection unchecked
            T[] page = (T[]) Array.newInstance(componentClass, intSize);
            tracker.add(sizeOfObjectArray(intSize));

            return new SingleHugeObjectArray<>(intSize, page);
        }

        private final int size;
        private T[] page;

        private SingleHugeObjectArray(int size, T[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public T get(long index) {
            assert index < size;
            return page[(int) index];
        }

        @Override
        public void set(long index, T value) {
            assert index < size;
            page[(int) index] = value;
        }

        @Override
        public T putIfAbsent(long index, Supplier<T> supplier) {
            assert index < size;
            T value;
            if ((value = page[(int) index]) == null) {
                if ((value = supplier.get()) != null) {
                    page[(int) index] = value;
                }
            }
            return value;
        }

        @Override
        public void setAll(LongFunction<T> gen) {
            Arrays.setAll(page, gen::apply);
        }

        @Override
        public void fill(T value) {
            Arrays.fill(page, value);
        }

        @Override
        public void copyTo(HugeObjectArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeObjectArray) {
                SingleHugeObjectArray dst = (SingleHugeObjectArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, null);
            } else if (dest instanceof PagedHugeObjectArray) {
                PagedHugeObjectArray dst = (PagedHugeObjectArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (Object[] dstPage : dst.pages) {
                    int toCopy = Math.min(remaining, dstPage.length);
                    if (toCopy == 0) {
                        Arrays.fill(page, null);
                    } else {
                        System.arraycopy(page, start, dstPage, 0, toCopy);
                        if (toCopy < dstPage.length) {
                            Arrays.fill(dstPage, toCopy, dstPage.length, null);
                        }
                        start += toCopy;
                        remaining -= toCopy;
                    }
                }
            }
        }

        @Override
        public HugeObjectArray<T> copyOf(long newLength, AllocationTracker tracker) {
            Class<T> tCls = (Class<T>) page.getClass().getComponentType();
            HugeObjectArray<T> copy = HugeObjectArray.newArray(tCls, newLength, tracker);
            this.copyTo(copy, newLength);
            return copy;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long sizeOf() {
            return sizeOfObjectArray(size);
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return sizeOfObjectArray(size);
            }
            return 0L;
        }

        @Override
        public HugeCursor<T[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public T[] toArray() {
            return page;
        }

        @Override
        public String toString() {
            return Arrays.toString(page);
        }
    }

    private static final class PagedHugeObjectArray<T> extends HugeObjectArray<T> {

        @SuppressWarnings("unchecked")
        private static <T> HugeObjectArray<T> of(Class<T> componentClass, long size, AllocationTracker tracker) {
            int numPages = numberOfPages(size);
            T[][] pages = (T[][]) Array.newInstance(componentClass, numPages, PAGE_SIZE);

            long memoryUsed = sizeOfObjectArray(numPages);
            final long pageBytes = sizeOfObjectArray(PAGE_SIZE);
            memoryUsed += ((numPages - 1) * pageBytes);
            final int lastPageSize = exclusiveIndexOfPage(size);
            pages[numPages - 1] = (T[]) Array.newInstance(componentClass, lastPageSize);
            memoryUsed += sizeOfObjectArray(lastPageSize);
            tracker.add(memoryUsed);

            return new PagedHugeObjectArray<>(size, pages, memoryUsed);
        }

        private final long size;
        private T[][] pages;
        private final long memoryUsed;

        private PagedHugeObjectArray(long size, T[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public T get(long index) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public void set(long index, T value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public T putIfAbsent(final long index, final Supplier<T> supplier) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            T[] page = pages[pageIndex];
            T value;
            if ((value = page[indexInPage]) == null) {
                if ((value = supplier.get()) != null) {
                    page[indexInPage] = value;
                }
            }
            return value;
        }

        @Override
        public void setAll(LongFunction<T> gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << PAGE_SHIFT;
                Arrays.setAll(pages[i], j -> gen.apply(t + j));
            }
        }

        @Override
        public void fill(T value) {
            for (T[] page : pages) {
                Arrays.fill(page, value);
            }
        }

        @Override
        public void copyTo(HugeObjectArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeObjectArray) {
                SingleHugeObjectArray dst = (SingleHugeObjectArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (T[] page : pages) {
                    int toCopy = Math.min(remaining, page.length);
                    if (toCopy == 0) {
                        break;
                    }
                    System.arraycopy(page, 0, dst.page, start, toCopy);
                    start += toCopy;
                    remaining -= toCopy;
                }
                Arrays.fill(dst.page, start, dst.size, null);
            } else if (dest instanceof PagedHugeObjectArray) {
                PagedHugeObjectArray dst = (PagedHugeObjectArray) dest;
                int pageLen = Math.min(pages.length, dst.pages.length);
                int lastPage = pageLen - 1;
                long remaining = length;
                for (int i = 0; i < lastPage; i++) {
                    T[] page = pages[i];
                    Object[] dstPage = dst.pages[i];
                    System.arraycopy(page, 0, dstPage, 0, page.length);
                    remaining -= page.length;
                }
                if (remaining > 0L) {
                    System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                    Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, null);
                }
                for (int i = pageLen; i < dst.pages.length; i++) {
                    Arrays.fill(dst.pages[i], null);
                }
            }
        }

        @Override
        public HugeObjectArray<T> copyOf(long newLength, AllocationTracker tracker) {
            Class<T> tCls = (Class<T>) pages.getClass().getComponentType().getComponentType();
            HugeObjectArray<T> copy = HugeObjectArray.newArray(tCls, newLength, tracker);
            this.copyTo(copy, newLength);
            return copy;
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
        public HugeCursor<T[]> newCursor() {
            return new HugeCursor.PagedCursor<>(size, pages);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T[] toArray() {
            return dumpToArray((Class<T[]>) pages.getClass().getComponentType());
        }
    }
}
