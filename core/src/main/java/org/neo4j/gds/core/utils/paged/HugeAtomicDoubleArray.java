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
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.mem.HugeArrays;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.function.DoubleUnaryOperator;

import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.gds.mem.HugeArrays.indexInPage;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.HugeArrays.pageIndex;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;

public abstract class HugeAtomicDoubleArray {

    /**
     * @return the double value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract double get(long index);

    /**
     * Atomically adds the given delta to the value at the given index.
     *
     * @param index the index
     * @param delta the value to add
     * @return the previous value at index
     */
    public abstract double getAndAdd(long index, double delta);

    /**
     * Sets the double value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, double value);

    /**
     * Atomically returns the double value at the given index and replaces it with the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract double getAndReplace(long index, double value);

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
    public abstract boolean compareAndSet(long index, double expect, double update);

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
    public abstract double compareAndExchange(long index, double expect, double update);

    /**
     * Atomically updates the element at index {@code index} with the results
     * of applying the given function, returning the updated value. The
     * function should be side-effect-free, since it may be re-applied
     * when attempted updates fail due to contention among threads.
     *
     * @param index          the index
     * @param updateFunction a side-effect-free function
     */
    public abstract void update(long index, DoubleUnaryOperator updateFunction);

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

    public abstract void setAll(double value);

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

    public DoubleNodePropertyValues asNodeProperties() {
        return new DoubleNodePropertyValues() {
            @Override
            public double doubleValue(long nodeId) {
                return get(nodeId);
            }

            @Override
            public long valuesStored() {
                return HugeAtomicDoubleArray.this.size();
            }
        };
    }

    /**
     * Creates a new array of the given size.
     */
    public static HugeAtomicDoubleArray newArray(long size) {
        return newArray(size, DoublePageCreator.passThrough(1));
    }

    /**
     * Creates a new array of the given size.
     * The values are pre-calculated according to the semantics of {@link java.util.Arrays#setAll(double[], java.util.function.IntToDoubleFunction)}
     */
    public static HugeAtomicDoubleArray newArray(
        long size,
        DoublePageCreator pageFiller
    ) {
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return HugeAtomicDoubleArray.SingleHugeAtomicDoubleArray.of(size, pageFiller);
        }
        return HugeAtomicDoubleArray.PagedHugeAtomicDoubleArray.of(size, pageFiller);
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;
        long instanceSize;
        long dataSize;
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            instanceSize = sizeOfInstance(HugeAtomicDoubleArray.SingleHugeAtomicDoubleArray.class);
            dataSize = sizeOfLongArray((int) size);
        } else {
            instanceSize = sizeOfInstance(HugeAtomicDoubleArray.PagedHugeAtomicDoubleArray.class);
            dataSize = HugeAtomicDoubleArray.PagedHugeAtomicDoubleArray.memoryUsageOfData(size);
        }
        return instanceSize + dataSize;
    }

    @TestOnly
    static HugeAtomicDoubleArray newPagedArray(
        long size,
        final DoublePageCreator pageFiller
    ) {
        return HugeAtomicDoubleArray.PagedHugeAtomicDoubleArray.of(size, pageFiller);
    }

    @TestOnly
    static HugeAtomicDoubleArray newSingleArray(
        int size,
        final DoublePageCreator pageFiller
    ) {
        return HugeAtomicDoubleArray.SingleHugeAtomicDoubleArray.of(size, pageFiller);
    }

    private static final class SingleHugeAtomicDoubleArray extends HugeAtomicDoubleArray {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);

        private static HugeAtomicDoubleArray of(
            long size,
            DoublePageCreator pageCreator
        ) {
            assert size <= HugeArrays.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            double[] page = new double[intSize];
            pageCreator.fillPage(page, 0);
            return new HugeAtomicDoubleArray.SingleHugeAtomicDoubleArray(intSize, page);
        }

        private final int size;
        private double[] page;

        private SingleHugeAtomicDoubleArray(int size, double[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public double get(long index) {
            return (double) ARRAY_HANDLE.getVolatile(page, (int) index);
        }

        @Override
        public double getAndAdd(long index, double delta) {
            double prev = (double) ARRAY_HANDLE.getAcquire(page, (int) index);
            while (true) {
                double next = prev + delta;
                double current = (double) ARRAY_HANDLE.compareAndExchangeRelease(page, (int) index, prev, next);
                if (Double.compare(prev, current) == 0) {
                    return prev;
                }
                prev = current;
            }
        }

        @Override
        public void set(long index, double value) {
            ARRAY_HANDLE.setVolatile(page, (int) index, value);
        }

        @Override
        public double getAndReplace(long index, double value) {
            double prev = (double) ARRAY_HANDLE.getAcquire(page, (int) index);
            while (true) {
                double current = (double) ARRAY_HANDLE.compareAndExchangeRelease(page, (int) index, prev, value);
                if (Double.compare(current, prev) == 0) {
                    return current;
                }
                prev = current;
            }
        }

        @Override
        public boolean compareAndSet(long index, double expect, double update) {
            return ARRAY_HANDLE.compareAndSet(page, (int) index, expect, update);
        }

        @Override
        public double compareAndExchange(long index, double expect, double update) {
            return (double) ARRAY_HANDLE.compareAndExchange(page, (int) index, expect, update);
        }

        @Override
        public void update(long index, DoubleUnaryOperator updateFunction) {
            double prev = (double) ARRAY_HANDLE.getAcquire(page, (int) index);
            while (true) {
                double next = updateFunction.applyAsDouble(prev);
                double current = (double) ARRAY_HANDLE.compareAndExchangeRelease(page, (int) index, prev, next);
                if (Double.compare(prev, current) == 0) {
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
        public void setAll(double value) {
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
    }

    static final class PagedHugeAtomicDoubleArray extends HugeAtomicDoubleArray {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);

        private static HugeAtomicDoubleArray of(
            long size,
            DoublePageCreator pageCreator
        ) {
            int numPages = numberOfPages(size);
            final int lastPageSize = exclusiveIndexOfPage(size);

            double[][] pages = new double[numPages][];
            pageCreator.fill(pages, lastPageSize);

            long memoryUsed = memoryUsageOfData(size);
            return new PagedHugeAtomicDoubleArray(size, pages, memoryUsed);
        }

        private static long memoryUsageOfData(long size) {
            int numberOfPages = numberOfPages(size);
            int numberOfFullPages = numberOfPages - 1;
            long bytesPerPage = sizeOfDoubleArray(PAGE_SIZE);
            int sizeOfLastPage = exclusiveIndexOfPage(size);
            long bytesOfLastPage = sizeOfDoubleArray(sizeOfLastPage);
            long memoryUsed = sizeOfObjectArray(numberOfPages);
            memoryUsed += (numberOfFullPages * bytesPerPage);
            memoryUsed += bytesOfLastPage;
            return memoryUsed;
        }

        private final long size;
        private double[][] pages;
        private final long memoryUsed;

        private PagedHugeAtomicDoubleArray(long size, double[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public double get(long index) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return (double) ARRAY_HANDLE.getVolatile(pages[pageIndex], indexInPage);
        }

        @Override
        public double getAndAdd(long index, double delta) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            double[] page = pages[pageIndex];
            double prev = (double) ARRAY_HANDLE.getAcquire(page, indexInPage);

            while (true) {
                double next = prev + delta;
                double current = (double) ARRAY_HANDLE.compareAndExchangeRelease(page, indexInPage, prev, next);
                if (Double.compare(prev, current) == 0) {
                    return prev;
                }
                prev = current;
            }
        }

        @Override
        public void set(long index, double value) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            ARRAY_HANDLE.setVolatile(pages[pageIndex], indexInPage, value);
        }

        @Override
        public double getAndReplace(long index, double value) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            double[] page = pages[pageIndex];
            double prev = (double) ARRAY_HANDLE.getAcquire(page, indexInPage);
            while (true) {
                double current = (double) ARRAY_HANDLE.compareAndExchangeRelease(page, indexInPage, prev, value);
                if (Double.compare(current, prev) == 0) {
                    return current;
                }
                prev = current;
            }
        }

        @Override
        public boolean compareAndSet(long index, double expect, double update) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return ARRAY_HANDLE.compareAndSet(pages[pageIndex], indexInPage, expect, update);
        }

        @Override
        public double compareAndExchange(long index, double expect, double update) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            return (double) ARRAY_HANDLE.compareAndExchange(pages[pageIndex], indexInPage, expect, update);
        }

        @Override
        public void update(long index, DoubleUnaryOperator updateFunction) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            double[] page = pages[pageIndex];
            double prev = (double) ARRAY_HANDLE.getAcquire(page, indexInPage);
            while (true) {
                double next = updateFunction.applyAsDouble(prev);
                double current = (double) ARRAY_HANDLE.compareAndExchangeRelease(page, indexInPage, prev, next);
                if (Double.compare(current, prev) == 0) {
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
            return memoryUsed;
        }

        @Override
        public void setAll(double value) {
            for (double[] page : pages) {
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
