/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.neo4j.graphalgo.compat.UnsafeProxy;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Arrays;
import java.util.function.IntToLongFunction;
import java.util.function.LongUnaryOperator;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.numberOfPages;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;

/**
 * A long-indexable version of a {@link java.util.concurrent.atomic.AtomicLongArray} that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller long-arrays ({@code long[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code long[]} might be used.
 *
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse.</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code long[]} does ({@code 0}).</li>
 * <li>It only supports a minimal subset of the atomic operations that {@link java.util.concurrent.atomic.AtomicLongArray} provides.</li>
 * </ul>
 *
 * <p><em>Basic Usage</em></p>
 * <pre>
 * {@code}
 * AllocationTracker tracker = ...;
 * long arraySize = 42L;
 * HugeAtomicLongArray array = HugeAtomicLongArray.newArray(arraySize, tracker);
 * array.set(13L, 37L);
 * long value = array.get(13L);
 * // value = 37L
 * {@code}
 * </pre>
 */
public abstract class HugeAtomicLongArray {

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
     * Atomically sets the element at position {@code index} to the given
     * updated value if the current value {@code ==} the expected value.
     *
     * @param index  the index
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful. False return indicates that
     *         the actual value was not equal to the expected value.
     */
    public abstract boolean compareAndSet(long index, long expect, long update);

    /**
     * Atomically updates the element at index {@code index} with the results
     * of applying the given function, returning the updated value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param index          the index
     * @param updateFunction a side-effect-free function
     */
    public abstract void update(long index, LongUnaryOperator updateFunction);

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    public abstract long size();

    /**
     * @return the amount of memory used by the instance of this array, in bytes.
     *         This should be the same as returned from {@link #release()} without actually releasing the array.
     */
    public abstract long sizeOf();

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that reference this array.
     * You have to {@link HugeCursor#close()} every cursor instance as well.
     * <p>
     * The amount is not removed from the {@link AllocationTracker} that had been provided in the constructor.
     *
     * @return the amount of memory freed, in bytes.
     */
    public abstract long release();

    /**
     * Creates a new array of the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static HugeAtomicLongArray newArray(long size, AllocationTracker tracker) {
        return newArray(size, PageFiller.passThrough(), tracker);
    }

    /**
     * Creates a new array of the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     * The values are pre-calculated according to the semantics of {@link Arrays#setAll(long[], IntToLongFunction)}
     */
    public static HugeAtomicLongArray newArray(long size, PageFiller pageFiller, AllocationTracker tracker) {
        if (size <= ArrayUtil.MAX_ARRAY_LENGTH) {
            return SingleHugeAtomicLongArray.of(size, pageFiller, tracker);
        }
        return PagedHugeAtomicLongArray.of(size, pageFiller, tracker);
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;
        long instanceSize;
        long dataSize;
        if (size <= ArrayUtil.MAX_ARRAY_LENGTH) {
            instanceSize = sizeOfInstance(SingleHugeAtomicLongArray.class);
            dataSize = sizeOfLongArray((int) size);
        } else {
            instanceSize = sizeOfInstance(PagedHugeAtomicLongArray.class);
            dataSize = PagedHugeAtomicLongArray.memoryUsageOfData(size);
        }
        return instanceSize + dataSize;
    }

    /* test-only */
    static HugeAtomicLongArray newPagedArray(long size, final PageFiller pageFiller, AllocationTracker tracker) {
        return PagedHugeAtomicLongArray.of(size, pageFiller, tracker);
    }

    /* test-only */
    static HugeAtomicLongArray newSingleArray(int size, final PageFiller pageFiller, AllocationTracker tracker) {
        return SingleHugeAtomicLongArray.of(size, pageFiller, tracker);
    }

    /**
     * A {@link PropertyTranslator} for instances of {@link HugeAtomicLongArray}s.
     */
    public static class Translator implements PropertyTranslator.OfLong<HugeAtomicLongArray> {

        public static final Translator INSTANCE = new Translator();

        @Override
        public long toLong(final HugeAtomicLongArray data, final long nodeId) {
            return data.get(nodeId);
        }
    }

    // implementation is similar to the AtomicLongArray, based on sun.misc.Unsafe
    // https://hg.openjdk.java.net/jdk/jdk/file/a1ee9743f4ee/jdk/src/share/classes/java/util/concurrent/atomic/AtomicLongArray.java
    // TODO: Replace usage of Unsafe with VarHandles once we can move to jdk9+
    // https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/src/java.base/share/classes/java/util/concurrent/atomic/AtomicLongArray.java

    // array-internal values to access the raw memory locations of certain elements
    // see #memoryOffset
    private static final int base;
    private static final int shift;

    static {
        UnsafeProxy.assertHasUnsafe();
        base = UnsafeProxy.arrayBaseOffset(long[].class);
        int scale = UnsafeProxy.arrayIndexScale(long[].class);
        if (!BitUtil.isPowerOfTwo(scale)) {
            throw new Error("data type scale not a power of two");
        }
        shift = 31 - Integer.numberOfLeadingZeros(scale);
    }

    private static long memoryOffset(int i) {
        return ((long) i << shift) + base;
    }

    private static final class SingleHugeAtomicLongArray extends HugeAtomicLongArray {

        private static HugeAtomicLongArray of(long size, PageFiller pageFiller, AllocationTracker tracker) {
            assert size <= ArrayUtil.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            tracker.add(sizeOfLongArray(intSize));
            long[] page = new long[intSize];
            pageFiller.accept(page);
            return new SingleHugeAtomicLongArray(intSize, page);
        }

        private final int size;
        private long[] page;

        private SingleHugeAtomicLongArray(int size, long[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public long get(long index) {
            assert index < size;
            return getRaw(memoryOffset((int) index));
        }

        @Override
        public void set(long index, long value) {
            assert index < size;
            UnsafeProxy.putLongVolatile(page, memoryOffset((int) index), value);
        }

        @Override
        public boolean compareAndSet(long index, long expect, long update) {
            assert index < size;
            return compareAndSetRaw(memoryOffset((int) index), expect, update);
        }

        @Override
        public void update(long index, LongUnaryOperator updateFunction) {
            assert index < size;
            long offset = memoryOffset((int) index);
            long prev, next;
            do {
                prev = getRaw(offset);
                next = updateFunction.applyAsLong(prev);
            } while (!compareAndSetRaw(offset, prev, next));
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

        private long getRaw(long offset) {
            return UnsafeProxy.getLongVolatile(page, offset);
        }

        private boolean compareAndSetRaw(long offset, long expect, long update) {
            return UnsafeProxy.compareAndSwapLong(page, offset, expect, update);
        }
    }

    private static final class PagedHugeAtomicLongArray extends HugeAtomicLongArray {


        private static HugeAtomicLongArray of(long size, PageFiller pageFiller, AllocationTracker tracker) {
            int numPages = numberOfPages(size);
            int lastPage = numPages - 1;
            final int lastPageSize = exclusiveIndexOfPage(size);

            long[][] pages = new long[numPages][];
            for (int i = 0; i < lastPage; i++) {
                pages[i] = new long[PAGE_SIZE];
                long base = ((long) i) << PAGE_SHIFT;
                pageFiller.accept(pages[i], base);
            }
            pages[lastPage] = new long[lastPageSize];
            long base = ((long) lastPage) << PAGE_SHIFT;
            pageFiller.accept(pages[lastPage], base);

            long memoryUsed = memoryUsageOfData(size);
            tracker.add(memoryUsed);
            return new PagedHugeAtomicLongArray(size, pages, memoryUsed);
        }

        private static long memoryUsageOfData(long size) {
            int numberOfPages = numberOfPages(size);
            int numberOfFullPages = numberOfPages - 1;
            long bytesPerPage = sizeOfLongArray(PAGE_SIZE);
            int sizeOfLastPast = exclusiveIndexOfPage(size);
            long bytesOfLastPage = sizeOfLongArray(sizeOfLastPast);
            long memoryUsed = sizeOfObjectArray(numberOfPages);
            memoryUsed += (numberOfFullPages * bytesPerPage);
            memoryUsed += bytesOfLastPage;
            return memoryUsed;
        }

        private final long size;
        private long[][] pages;
        private final long memoryUsed;

        private PagedHugeAtomicLongArray(long size, long[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public long get(long index) {
            assert index < size && index >= 0;
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return getRaw(pages[pageIndex], memoryOffset(indexInPage));
        }

        @Override
        public void set(long index, long value) {
            assert index < size && index >= 0;
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            UnsafeProxy.putLongVolatile(pages[pageIndex], memoryOffset(indexInPage), value);
        }

        @Override
        public boolean compareAndSet(long index, long expect, long update) {
            assert index < size && index >= 0;
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return compareAndSetRaw(pages[pageIndex], memoryOffset(indexInPage), expect, update);
        }

        @Override
        public void update(long index, LongUnaryOperator updateFunction) {
            assert index < size && index >= 0;
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            long[] page = pages[pageIndex];
            long offset = memoryOffset(indexInPage);
            long prev, next;
            do {
                prev = getRaw(page, offset);
                next = updateFunction.applyAsLong(prev);
            } while (!compareAndSetRaw(page, offset, prev, next));
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

        private long getRaw(long[] page, long offset) {
            return UnsafeProxy.getLongVolatile(page, offset);
        }

        private boolean compareAndSetRaw(long[] page, long offset, long expect, long update) {
            return UnsafeProxy.compareAndSwapLong(page, offset, expect, update);
        }
    }
}
