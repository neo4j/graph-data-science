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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.collections.cursor.HugeCursorSupport;
import org.neo4j.gds.mem.MemoryUsage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static org.neo4j.gds.mem.HugeArrays.MAX_ARRAY_LENGTH;
import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.gds.mem.HugeArrays.indexInPage;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.HugeArrays.pageIndex;

/**
 * A long-indexable array of atomic bytes that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller byte-arrays ({@code byte[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code byte[]} might be used.
 *
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse.</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code byte[]} does ({@code 0}).</li>
 * <li>It only supports a minimal subset of the atomic operations that {@link java.util.concurrent.atomic.AtomicLongArray} provides.</li>
 * </ul>
 *
 * <p><em>Basic Usage</em></p>
 * <pre>
 * {@code}
 * AllocationTracker allocationTracker = ...;
 * long arraySize = 42L;
 * HugeAtomicByteArray array = HugeAtomicByteArray.newArray(arraySize, allocationTracker);
 * array.set(13L, (byte)37);
 * byte value = array.get(13L);
 * // value = (byte)37;
 * {@code}
 * </pre>
 *
 * Implementation is similar to the AtomicLongArray (but for bytes), based on sun.misc.Unsafe
 * https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/src/java.base/share/classes/java/util/concurrent/atomic/AtomicLongArray.java
 */
public abstract class HugeAtomicByteArray implements HugeCursorSupport<byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract HugeCursor<byte[]> newCursor();

    /**
     * @return the byte value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract byte get(long index);

    /**
     * Atomically adds the given delta to the value at the given index.
     *
     * @param index the index
     * @param delta the value to add
     * @return the previous value at index
     */
    public abstract byte getAndAdd(long index, byte delta);

    /**
     * Sets the byte value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, byte value);

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
    public abstract boolean compareAndSet(long index, byte expect, byte update);

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
     *     which will be the same as the expected value if successful
     *     or the new current value if unsuccessful.
     */
    public abstract byte compareAndExchange(long index, byte expect, byte update);

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
    public abstract void setAll(byte value);

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

    /**
     * Creates a new array of the given size.
     */
    public static HugeAtomicByteArray newArray(long size) {
        if (size <= MAX_ARRAY_LENGTH) {
            return SingleHugeAtomicByteArray.of(size);
        }
        return PagedHugeAtomicByteArray.of(size, BytePageCreator.of(1));
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;
        long instanceSize;
        long dataSize;
        if (size <= MAX_ARRAY_LENGTH) {
            instanceSize = MemoryUsage.sizeOfInstance(SingleHugeAtomicByteArray.class);
            dataSize = MemoryUsage.sizeOfByteArray((int) size);
        } else {
            instanceSize = MemoryUsage.sizeOfInstance(PagedHugeAtomicByteArray.class);
            dataSize = PagedHugeAtomicByteArray.memoryUsageOfData(size);
        }
        return instanceSize + dataSize;
    }

    @TestOnly
    static HugeAtomicByteArray newPagedArray(
        long size,
        final BytePageCreator pageFiller
    ) {
        return HugeAtomicByteArray.PagedHugeAtomicByteArray.of(size, pageFiller);
    }

    @TestOnly
    static HugeAtomicByteArray newSingleArray(int size) {
        return HugeAtomicByteArray.SingleHugeAtomicByteArray.of(size);
    }

    static final class SingleHugeAtomicByteArray extends HugeAtomicByteArray {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(byte[].class);

        private static HugeAtomicByteArray of(long size) {
            assert size <= MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            byte[] page = new byte[intSize];

            return new SingleHugeAtomicByteArray(intSize, page);
        }

        private final int size;
        private byte[] page;

        private SingleHugeAtomicByteArray(int size, byte[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public HugeCursor<byte[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public byte get(long index) {
            return (byte) ARRAY_HANDLE.getVolatile(page, (int) index);
        }

        @Override
        public byte getAndAdd(long index, byte delta) {
            byte prev = (byte) ARRAY_HANDLE.getAcquire(page, (int) index);
            while (true) {
                byte next = (byte) (prev + delta);
                byte current = (byte) ARRAY_HANDLE.compareAndExchangeRelease(page, (int) index, prev, next);
                if (prev == current) {
                    return prev;
                }
                prev = current;
            }
        }

        @Override
        public void set(long index, byte value) {
            ARRAY_HANDLE.setVolatile(page, (int) index, value);
        }

        @Override
        public boolean compareAndSet(long index, byte expect, byte update) {
            return ARRAY_HANDLE.compareAndSet(page, (int) index, expect, update);
        }

        @Override
        public byte compareAndExchange(long index, byte expect, byte update) {
            return (byte) ARRAY_HANDLE.compareAndExchange(page, (int) index, expect, update);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long sizeOf() {
            return MemoryUsage.sizeOfByteArray(size);
        }

        @Override
        public void setAll(byte value) {
            Arrays.fill(page, value);
            VarHandle.storeStoreFence();
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return MemoryUsage.sizeOfByteArray(size);
            }
            return 0L;
        }
    }

    static final class PagedHugeAtomicByteArray extends HugeAtomicByteArray {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(byte[].class);

        private static HugeAtomicByteArray of(
            long size,
            BytePageCreator pageCreator
        ) {
            int numPages = numberOfPages(size);
            final int lastPageSize = exclusiveIndexOfPage(size);

            byte[][] pages = new byte[numPages][];
            pageCreator.fill(pages, lastPageSize);

            long memoryUsed = memoryUsageOfData(size);

            return new PagedHugeAtomicByteArray(size, pages, memoryUsed);
        }

        private static long memoryUsageOfData(long size) {
            int numberOfPages = numberOfPages(size);
            int numberOfFullPages = numberOfPages - 1;
            long bytesPerPage = MemoryUsage.sizeOfByteArray(PAGE_SIZE);
            int sizeOfLastPage = exclusiveIndexOfPage(size);
            long bytesOfLastPage = MemoryUsage.sizeOfByteArray(sizeOfLastPage);
            long memoryUsed = MemoryUsage.sizeOfObjectArray(numberOfPages);
            memoryUsed += (numberOfFullPages * bytesPerPage);
            memoryUsed += bytesOfLastPage;
            return memoryUsed;
        }

        private final long size;
        private byte[][] pages;
        private final long memoryUsed;

        private PagedHugeAtomicByteArray(long size, byte[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public byte get(long index) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return (byte) ARRAY_HANDLE.getVolatile(pages[pageIndex], indexInPage);
        }

        @Override
        public byte getAndAdd(long index, byte delta) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            byte[] page = pages[pageIndex];
            byte prev = (byte) ARRAY_HANDLE.getAcquire(page, indexInPage);

            while (true) {
                byte next = (byte) (prev + delta);
                byte current = (byte) ARRAY_HANDLE.compareAndExchangeRelease(page, indexInPage, prev, next);
                if (prev == current) {
                    return prev;
                }
                prev = current;
            }
        }

        @Override
        public void set(long index, byte value) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            ARRAY_HANDLE.setVolatile(pages[pageIndex], indexInPage, value);
        }

        @Override
        public boolean compareAndSet(long index, byte expect, byte update) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return ARRAY_HANDLE.compareAndSet(pages[pageIndex], indexInPage, expect, update);
        }

        @Override
        public byte compareAndExchange(long index, byte expect, byte update) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return (byte) ARRAY_HANDLE.compareAndExchange(pages[pageIndex], indexInPage, expect, update);
        }

        @Override
        public HugeCursor<byte[]> newCursor() {
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
        public void setAll(byte value) {
            for (byte[] page : pages) {
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
    }

}
