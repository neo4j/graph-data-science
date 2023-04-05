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

import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.HugeArrays;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import static org.neo4j.gds.mem.HugeArrays.PAGE_SHIFT;
import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.gds.mem.HugeArrays.indexInPage;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.HugeArrays.pageIndex;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;

/**
 * A long-indexable version of a Object array ({@code T[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller object-arrays ({@code T[][]}) to support approx. 32k bn. elements.
 * If the provided size is small enough, an optimized view of a single {@code T[]} might be used.
 *
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code T[]} does ({@code null}).</li>
 * </ul>
 *
 * <p><em>Basic Usage</em></p>
 * <pre>
 * {@code}
 * AllocationTracker allocationTracker = ...;
 * long arraySize = 42L;
 * HugeObjectArray&lt;String&gt; array = HugeObjectArray.newArray(String.class, arraySize, allocationTracker);
 * array.set(13L, "37");
 * String value = array.get(13L);
 * // value = "37"
 * {@code}
 * </pre>
 */
public abstract class HugeObjectArray<T> extends HugeArray<T[], T, HugeObjectArray<T>> {

    /**
     * @return the value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract T get(long index);

    /**
     * Returns the value at the given index. If the value at the index is {@code null},
     * the given defaultValue is returned.
     *
     * @param defaultValue return value in case the element at index is {@code null}
     * @return value at index or defaultValue
     */
    public abstract T getOrDefault(long index, T defaultValue);

    /**
     * Sets the value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, T value);

    /**
     * If the value at the given index is {@code null}, attempts to compute its value using
     * the given supplier and enters it into this array unless {@code null}.
     *
     * @param index index at which the specified value is to be associated
     * @return the current (existing or computed) value associated with
     *         the specified index, or null if the computed value is null
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract T putIfAbsent(long index, Supplier<T> supplier);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(Object[], java.util.function.IntFunction)}.
     */
    public abstract void setAll(LongFunction<T> gen);

    /**
     * Assigns the specified value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(Object[], Object)}.
     */
    public abstract void fill(T value);

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
    public abstract HugeCursor<T[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void copyTo(final HugeObjectArray<T> dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract HugeObjectArray<T> copyOf(final long newLength);

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
    public abstract T[] toArray();

    @Override
    public NodePropertyValues asNodeProperties() {
        var cls = elementClass();
        if (cls == float[].class) {
            return new FloatArrayNodePropertyValues() {
                @Override
                public float[] floatArrayValue(long nodeId) {
                    return (float[]) get(nodeId);
                }

                @Override
                public long nodeCount() {
                    return HugeObjectArray.this.size();
                }
            };
        }
        if (cls == double[].class) {
            return new DoubleArrayNodePropertyValues() {
                @Override
                public double[] doubleArrayValue(long nodeId) {
                    return (double[]) get(nodeId);
                }

                @Override
                public long nodeCount() {
                    return HugeObjectArray.this.size();
                }
            };
        }
        if (cls == long[].class) {
            return new LongArrayNodePropertyValues() {
                @Override
                public long[] longArrayValue(long nodeId) {
                    return (long[]) get(nodeId);
                }

                @Override
                public long nodeCount() {
                    return HugeObjectArray.this.size();
                }
            };
        }
        throw new UnsupportedOperationException("This HugeObjectArray can not be converted to node properties.");
    }

    abstract Class<T> elementClass();

    /**
     * Creates a new array of the given size.
     */
    public static <T> HugeObjectArray<T> newArray(
        Class<T> componentClass,
        long size
    ) {
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return SingleHugeObjectArray.of(componentClass, size);
        }
        return PagedHugeObjectArray.of(componentClass, size);
    }

    @SafeVarargs
    public static <T> HugeObjectArray<T> of(final T... values) {
        return new HugeObjectArray.SingleHugeObjectArray<>(values.length, values);
    }

    /* test-only */
    static <T> HugeObjectArray<T> newPagedArray(
        Class<T> componentClass,
        long size
    ) {
        return PagedHugeObjectArray.of(componentClass, size);
    }

    /* test-only */
    static <T> HugeObjectArray<T> newSingleArray(
        Class<T> componentClass,
        int size
    ) {
        return SingleHugeObjectArray.of(componentClass, size);
    }

    public static long memoryEstimation(long arraySize, long objectSize) {
        var sizeOfInstance = arraySize <= HugeArrays.MAX_ARRAY_LENGTH
            ? sizeOfInstance(SingleHugeObjectArray.class)
            : sizeOfInstance(PagedHugeObjectArray.class);

        int numPages = numberOfPages(arraySize);

        long outArrayMemoryUsage = sizeOfObjectArray(numPages);

        long memoryUsagePerPage = sizeOfObjectArray(PAGE_SIZE) + (PAGE_SIZE * objectSize);
        long pageMemoryUsage = (numPages - 1) * memoryUsagePerPage;

        int lastPageSize = exclusiveIndexOfPage(arraySize);
        var lastPageMemoryUsage = sizeOfObjectArray(lastPageSize) + (lastPageSize * objectSize);

        return sizeOfInstance + outArrayMemoryUsage + pageMemoryUsage + lastPageMemoryUsage;
    }

    // TODO: let's remove this method
    public static MemoryEstimation memoryEstimation(MemoryEstimation objectEstimation) {
        var builder = MemoryEstimations.builder();

        builder.perNode("instance", nodeCount -> {
            if (nodeCount <= HugeArrays.MAX_ARRAY_LENGTH) {
                return sizeOfInstance(SingleHugeObjectArray.class);
            } else {
                return sizeOfInstance(PagedHugeObjectArray.class);
            }
        });

        builder.perNode("data", objectEstimation);

        builder.perNode("pages", nodeCount -> {
            if (nodeCount <= HugeArrays.MAX_ARRAY_LENGTH) {
                return sizeOfObjectArray(nodeCount);
            } else {
                int numPages = numberOfPages(nodeCount);
                return sizeOfObjectArray(numPages) + numPages * sizeOfObjectArray(PAGE_SIZE);
            }
        });
        return builder.build();
    }

    // TODO: lets remove this method
    public static MemoryEstimation memoryEstimation(long objectEstimation) {
        return memoryEstimation(
            MemoryEstimations.of("instance", (dimensions, concurrency) -> MemoryRange.of(objectEstimation))
        );
    }

    private static final class SingleHugeObjectArray<T> extends HugeObjectArray<T> {

        private static <T> HugeObjectArray<T> of(
            Class<T> componentClass,
            long size
        ) {
            assert size <= HugeArrays.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            //noinspection unchecked
            T[] page = (T[]) Array.newInstance(componentClass, intSize);

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
        public T getOrDefault(long index, T defaultValue) {
            return Objects.requireNonNullElse(get(index), defaultValue);
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
        public void copyTo(HugeObjectArray<T> dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeObjectArray) {
                SingleHugeObjectArray<T> dst = (SingleHugeObjectArray<T>) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, null);
            } else if (dest instanceof PagedHugeObjectArray) {
                PagedHugeObjectArray<T> dst = (PagedHugeObjectArray<T>) dest;
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
        public HugeObjectArray<T> copyOf(long newLength) {
            Class<T> tCls = (Class<T>) page.getClass().getComponentType();
            HugeObjectArray<T> copy = HugeObjectArray.newArray(tCls, newLength);
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

        @Override
        Class<T> elementClass() {
            return (Class<T>) page.getClass().getComponentType();
        }
    }

    private static final class PagedHugeObjectArray<T> extends HugeObjectArray<T> {

        @SuppressWarnings("unchecked")
        private static <T> HugeObjectArray<T> of(
            Class<T> componentClass,
            long size
        ) {
            int numPages = numberOfPages(size);
            T[][] pages = (T[][]) Array.newInstance(componentClass, numPages, PAGE_SIZE);

            long memoryUsed = sizeOfObjectArray(numPages);
            final long pageBytes = sizeOfObjectArray(PAGE_SIZE);
            memoryUsed += ((numPages - 1) * pageBytes);
            final int lastPageSize = exclusiveIndexOfPage(size);
            pages[numPages - 1] = (T[]) Array.newInstance(componentClass, lastPageSize);
            memoryUsed += sizeOfObjectArray(lastPageSize);

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
        public T getOrDefault(long index, T defaultValue) {
            return Objects.requireNonNullElse(get(index), defaultValue);
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
        public void copyTo(HugeObjectArray<T> dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeObjectArray) {
                SingleHugeObjectArray<T> dst = (SingleHugeObjectArray<T>) dest;
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
                PagedHugeObjectArray<T> dst = (PagedHugeObjectArray<T>) dest;
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
        public HugeObjectArray<T> copyOf(long newLength) {
            Class<T> tCls = (Class<T>) pages.getClass().getComponentType().getComponentType();
            HugeObjectArray<T> copy = HugeObjectArray.newArray(tCls, newLength);
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

        @Override
        Class<T> elementClass() {
            return (Class<T>) pages.getClass().getComponentType().getComponentType();
        }
    }
}
