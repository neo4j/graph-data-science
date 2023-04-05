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
import org.neo4j.gds.collections.cursor.HugeCursorSupport;
import org.neo4j.gds.mem.HugeArrays;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.gds.mem.HugeArrays.indexInPage;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.HugeArrays.pageIndex;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;

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
 * AllocationTracker allocationTracker = ...;
 * long arraySize = 42L;
 * HugeAtomicLongArray array = HugeAtomicLongArray.newArray(arraySize, allocationTracker);
 * array.set(13L, 37L);
 * long value = array.get(13L);
 * // value = 37L
 * {@code}
 * </pre>
 *
 * Implementation is similar to the AtomicLongArray, based on sun.misc.Unsafe
 * https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/src/java.base/share/classes/java/util/concurrent/atomic/AtomicLongArray.java
 */
public abstract class HugeAtomicLongArray implements HugeCursorSupport<long[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract HugeCursor<long[]> newCursor();

    /**
     * @return the long value at the given index (volatile)
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract long get(long index);

    /**
     * Atomically adds the given delta to the value at the given index.
     *
     * @param index the index
     * @param delta the value to add
     * @return the previous value at index
     */
    public abstract long getAndAdd(long index, long delta);

    /**
     * Sets the long value at the given index to the given value (volatile).
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
     *     the actual value was not equal to the expected value.
     */
    public abstract boolean compareAndSet(long index, long expect, long update);

    /**
     * Atomically sets the element at position {@code index} to the given
     * updated value if the current value, referred to as the <em>witness value</em>,
     * {@code ==} the expected value.
     *
     * This operation works as if implemented as
     *
     * <pre>
     *     if (this.compareAndSet(index, expect, update)) {
     *         return expect;
     *     } else {
     *         return this.get(index);
     *     }
     * </pre>
     *
     * The actual implementation is done with a single atomic operation so that the
     * returned witness value is the value that was failing the update, not one that
     * needs be read again after the failed update.
     *
     * This allows one to write CAS-loops in a different way, which removes
     * one volatile read per loop iteration
     *
     * <pre>
     *     var oldValue = this.get(index);
     *     while (true) {
     *         var newValue = updateFunction(oldValue);
     *         var witnessValue = this.compareAndExchange(index, oldValue, newValue);
     *         if (witnessValue == oldValue) {
     *             // update successful
     *             break;
     *         }
     *         // update unsuccessful set, loop and try again.
     *         // Here we already have the updated witness value and don't need to issue
     *         // a new read
     *         oldValue = witnessValue;
     *     }
     * </pre>
     *
     * @param index  the index
     * @param expect the expected value
     * @param update the new value
     * @return the result that is the witness value,
     *         which will be the same as the expected value if successful
     *         or the new current value if unsuccessful.
     */
    public abstract long compareAndExchange(long index, long expect, long update);

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
     *     This should be the same as returned from {@link #release()} without actually releasing the array.
     */
    public abstract long sizeOf();

    /**
     * Set all entries in the array to the given value.
     * This method is not atomic!
     */
    public abstract void setAll(long value);

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that reference this array.
     * You have to {@link HugeCursor#close()} every cursor instance as well.
     *
     * @return the amount of memory freed, in bytes.
     */
    public abstract long release();

    public LongNodePropertyValues asNodeProperties() {
        return new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return get(nodeId);
            }

            @Override
            public long nodeCount() {
                return HugeAtomicLongArray.this.size();
            }
        };
    }

    public abstract void copyTo(HugeAtomicLongArray dest, long length);

    /**
     * Creates a new array of the given size.
     */
    public static HugeAtomicLongArray newArray(long size) {
        return newArray(size, LongPageCreator.passThrough(1));
    }

    /**
     * Creates a new array of the given size.
     * The values are pre-calculated according to the semantics of {@link java.util.Arrays#setAll(long[], java.util.function.IntToLongFunction)}
     */
    public static HugeAtomicLongArray newArray(
        long size,
        LongPageCreator pageFiller
    ) {
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return SingleHugeAtomicLongArray.of(size, pageFiller);
        }
        return PagedHugeAtomicLongArray.of(size, pageFiller);
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;
        long instanceSize;
        long dataSize;
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            instanceSize = sizeOfInstance(SingleHugeAtomicLongArray.class);
            dataSize = sizeOfLongArray((int) size);
        } else {
            instanceSize = sizeOfInstance(PagedHugeAtomicLongArray.class);
            dataSize = PagedHugeAtomicLongArray.memoryUsageOfData(size);
        }
        return instanceSize + dataSize;
    }

    /* test-only */
    static HugeAtomicLongArray newPagedArray(
        long size,
        final LongPageCreator pageFiller
    ) {
        return PagedHugeAtomicLongArray.of(size, pageFiller);
    }

    /* test-only */
    static HugeAtomicLongArray newSingleArray(
        int size,
        final LongPageCreator pageFiller
    ) {
        return SingleHugeAtomicLongArray.of(size, pageFiller);
    }

    static final class SingleHugeAtomicLongArray extends HugeAtomicLongArray {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        private static HugeAtomicLongArray of(
            long size,
            LongPageCreator pageCreator
        ) {
            assert size <= HugeArrays.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            long[] page = new long[intSize];
            pageCreator.fillPage(page, 0);
            return new SingleHugeAtomicLongArray(intSize, page);
        }

        private final int size;
        private long[] page;

        private SingleHugeAtomicLongArray(int size, long[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public HugeCursor<long[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public long get(long index) {
            return (long) ARRAY_HANDLE.getVolatile(page, (int) index);
        }

        @Override
        public long getAndAdd(long index, long delta) {
            long prev = (long) ARRAY_HANDLE.getAcquire(page, (int) index);
            while (true) {
                long next = prev + delta;
                long current = (long) ARRAY_HANDLE.compareAndExchangeRelease(page, (int) index, prev, next);
                if (prev == current) {
                    return prev;
                }
                prev = current;
            }
        }

        @Override
        public void set(long index, long value) {
            ARRAY_HANDLE.setVolatile(page, (int) index, value);
        }

        @Override
        public boolean compareAndSet(long index, long expect, long update) {
            return ARRAY_HANDLE.compareAndSet(page, (int) index, expect, update);
        }

        @Override
        public long compareAndExchange(long index, long expect, long update) {
            return (long) ARRAY_HANDLE.compareAndExchange(page, (int) index, expect, update);
        }

        @Override
        public void update(long index, LongUnaryOperator updateFunction) {
            long prev = (long) ARRAY_HANDLE.getAcquire(page, (int) index);
            while (true) {
                long next = updateFunction.applyAsLong(prev);
                long current = (long) ARRAY_HANDLE.compareAndExchangeRelease(page, (int) index, prev, next);
                if (prev == current) {
                    return;
                }
                prev = current;
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
        public void setAll(long value) {
            Arrays.fill(page, value);
            VarHandle.storeStoreFence();
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
        public void copyTo(HugeAtomicLongArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof HugeAtomicLongArray.SingleHugeAtomicLongArray) {
                HugeAtomicLongArray.SingleHugeAtomicLongArray dst = (HugeAtomicLongArray.SingleHugeAtomicLongArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, 0L);
            } else if (dest instanceof HugeAtomicLongArray.PagedHugeAtomicLongArray) {
                HugeAtomicLongArray.PagedHugeAtomicLongArray dst = (HugeAtomicLongArray.PagedHugeAtomicLongArray) dest;
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
    }

    static final class PagedHugeAtomicLongArray extends HugeAtomicLongArray {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        private static HugeAtomicLongArray of(
            long size,
            LongPageCreator pageCreator
        ) {
            int numPages = numberOfPages(size);
            final int lastPageSize = exclusiveIndexOfPage(size);

            long[][] pages = new long[numPages][];
            pageCreator.fill(pages, lastPageSize);

            long memoryUsed = memoryUsageOfData(size);
            return new PagedHugeAtomicLongArray(size, pages, memoryUsed);
        }

        private static long memoryUsageOfData(long size) {
            int numberOfPages = numberOfPages(size);
            int numberOfFullPages = numberOfPages - 1;
            long bytesPerPage = sizeOfLongArray(PAGE_SIZE);
            int sizeOfLastPage = exclusiveIndexOfPage(size);
            long bytesOfLastPage = sizeOfLongArray(sizeOfLastPage);
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
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return (long) ARRAY_HANDLE.getVolatile(pages[pageIndex], indexInPage);
        }

        @Override
        public long getAndAdd(long index, long delta) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            long[] page = pages[pageIndex];
            long prev = (long) ARRAY_HANDLE.getAcquire(page, indexInPage);

            while (true) {
                long next = prev + delta;
                long current = (long) ARRAY_HANDLE.compareAndExchangeRelease(page, indexInPage, prev, next);
                if (prev == current) {
                    return prev;
                }
                prev = current;
            }
        }

        @Override
        public void set(long index, long value) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            ARRAY_HANDLE.setVolatile(pages[pageIndex], indexInPage, value);
        }

        @Override
        public boolean compareAndSet(long index, long expect, long update) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return ARRAY_HANDLE.compareAndSet(pages[pageIndex], indexInPage, expect, update);
        }

        @Override
        public long compareAndExchange(long index, long expect, long update) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return (long) ARRAY_HANDLE.compareAndExchange(pages[pageIndex], indexInPage, expect, update);
        }

        @Override
        public void update(long index, LongUnaryOperator updateFunction) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            long[] page = pages[pageIndex];
            long prev = (long) ARRAY_HANDLE.getAcquire(page, indexInPage);
            while (true) {
                long next = updateFunction.applyAsLong(prev);
                long current = (long) ARRAY_HANDLE.compareAndExchangeRelease(page, indexInPage, prev, next);
                if (prev == current) {
                    return;
                }
                prev = current;
            }
        }

        @Override
        public HugeCursor<long[]> newCursor() {
            return new HugeCursor.PagedCursor<>(size, pages);
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
        public void setAll(long value) {
            for (long[] page : pages) {
                Arrays.fill(page, value);
            }
            VarHandle.storeStoreFence();
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
        public void copyTo(HugeAtomicLongArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof HugeAtomicLongArray.SingleHugeAtomicLongArray) {
                HugeAtomicLongArray.SingleHugeAtomicLongArray dst = (HugeAtomicLongArray.SingleHugeAtomicLongArray) dest;
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
            } else if (dest instanceof HugeAtomicLongArray.PagedHugeAtomicLongArray) {
                HugeAtomicLongArray.PagedHugeAtomicLongArray dst = (HugeAtomicLongArray.PagedHugeAtomicLongArray) dest;
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

    }

}
