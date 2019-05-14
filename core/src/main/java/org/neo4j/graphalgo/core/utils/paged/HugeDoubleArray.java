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
import java.util.function.IntToDoubleFunction;
import java.util.function.LongFunction;
import java.util.function.LongToDoubleFunction;

import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.SINGLE_PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.numberOfPages;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

/**
 * A long-indexable version of a primitive double array ({@code double[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller double-arrays ({@code double[][]}) to support approx. 32k bn. elements.
 * If the the provided size is small enough, an optimized view of a single {@code double[]} might be used.
 * <p>
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse.</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code double[]} does ({@code 0}).</li>
 * </ul>
 * <p>
 * <h3>Basic Usage</h3>
 * <pre>
 * {@code}
 * AllocationTracker tracker = ...;
 * long arraySize = 42L;
 * HugeDoubleArray array = HugeDoubleArray.newArray(arraySize, tracker);
 * array.set(13L, 37D);
 * double value = array.get(13L);
 * // value = 37D
 * {@code}
 * </pre>
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class HugeDoubleArray extends HugeArray<double[], Double, HugeDoubleArray> {

    /**
     * @return the double value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public double get(long index);

    /**
     * Sets the double value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void set(long index, double value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract public void addTo(long index, double value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(double[], IntToDoubleFunction)}.
     */
    abstract public void setAll(LongToDoubleFunction gen);

    /**
     * Assigns the specified double value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(double[], double)}.
     */
    abstract public void fill(double value);

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
    abstract public HugeCursor<double[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    abstract public void copyTo(final HugeDoubleArray dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    public final HugeDoubleArray copyOf(final long newLength, final AllocationTracker tracker) {
        HugeDoubleArray copy = HugeDoubleArray.newArray(newLength, tracker);
        this.copyTo(copy, newLength);
        return copy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final Double boxedGet(final long index) {
        return get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSet(final long index, final Double value) {
        set(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSetAll(final LongFunction<Double> gen) {
        setAll(gen::apply);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedFill(final Double value) {
        fill(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double[] toArray() {
        return dumpToArray(double[].class);
    }

    /**
     * Creates a new array if the given size, tracking the memory requirements into the given {@link AllocationTracker}.
     * The tracker is no longer referenced, as the arrays do not dynamically change their size.
     */
    public static HugeDoubleArray newArray(long size, AllocationTracker tracker) {
        if (size <= SINGLE_PAGE_SIZE) {
            return SingleHugeDoubleArray.of(size, tracker);
        }
        return PagedHugeDoubleArray.of(size, tracker);
    }

    public static HugeDoubleArray of(final double... values) {
        return new HugeDoubleArray.SingleHugeDoubleArray(values.length, values);
    }

    /* test-only */
    static HugeDoubleArray newPagedArray(long size, AllocationTracker tracker) {
        return PagedHugeDoubleArray.of(size, tracker);
    }

    /* test-only */
    static HugeDoubleArray newSingleArray(int size, AllocationTracker tracker) {
        return SingleHugeDoubleArray.of(size, tracker);
    }

    /**
     * A {@link PropertyTranslator} for instances of {@link HugeDoubleArray}s.
     */
    public static class Translator implements PropertyTranslator.OfDouble<HugeDoubleArray> {

        public static final Translator INSTANCE = new Translator();

        @Override
        public double toDouble(final HugeDoubleArray data, final long nodeId) {
            return data.get(nodeId);
        }
    }

    private static final class SingleHugeDoubleArray extends HugeDoubleArray {

        private static HugeDoubleArray of(long size, AllocationTracker tracker) {
            assert size <= SINGLE_PAGE_SIZE;
            final int intSize = (int) size;
            double[] page = new double[intSize];
            tracker.add(sizeOfDoubleArray(intSize));

            return new SingleHugeDoubleArray(intSize, page);
        }

        private final int size;
        private double[] page;

        private SingleHugeDoubleArray(int size, double[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public double get(long index) {
            assert index < size;
            return page[(int) index];
        }

        @Override
        public void set(long index, double value) {
            assert index < size;
            page[(int) index] = value;
        }

        @Override
        public void addTo(long index, double value) {
            assert index < size;
            page[(int) index] += value;
        }

        @Override
        public void setAll(LongToDoubleFunction gen) {
            Arrays.setAll(page, gen::applyAsDouble);
        }

        @Override
        public void fill(double value) {
            Arrays.fill(page, value);
        }

        @Override
        public void copyTo(HugeDoubleArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeDoubleArray) {
                SingleHugeDoubleArray dst = (SingleHugeDoubleArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, 0D);
            } else if (dest instanceof PagedHugeDoubleArray) {
                PagedHugeDoubleArray dst = (PagedHugeDoubleArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (double[] dstPage : dst.pages) {
                    int toCopy = Math.min(remaining, dstPage.length);
                    if (toCopy == 0) {
                        Arrays.fill(page, 0D);
                    } else {
                        System.arraycopy(page, start, dstPage, 0, toCopy);
                        if (toCopy < dstPage.length) {
                            Arrays.fill(dstPage, toCopy, dstPage.length, 0D);
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
            return sizeOfDoubleArray(size);
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return sizeOfDoubleArray(size);
            }
            return 0L;
        }

        @Override
        public HugeCursor<double[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public double[] toArray() {
            return page;
        }

        @Override
        public String toString() {
            return Arrays.toString(page);
        }
    }

    private static final class PagedHugeDoubleArray extends HugeDoubleArray {

        private static HugeDoubleArray of(long size, AllocationTracker tracker) {
            int numPages = numberOfPages(size);
            double[][] pages = new double[numPages][];

            long memoryUsed = sizeOfObjectArray(numPages);
            final long pageBytes = sizeOfDoubleArray(PAGE_SIZE);
            for (int i = 0; i < numPages - 1; i++) {
                memoryUsed += pageBytes;
                pages[i] = new double[PAGE_SIZE];
            }
            final int lastPageSize = exclusiveIndexOfPage(size);
            pages[numPages - 1] = new double[lastPageSize];
            memoryUsed += sizeOfDoubleArray(lastPageSize);
            tracker.add(memoryUsed);

            return new PagedHugeDoubleArray(size, pages, memoryUsed);
        }

        private final long size;
        private double[][] pages;
        private final long memoryUsed;

        private PagedHugeDoubleArray(long size, double[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public double get(long index) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public void set(long index, double value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public void addTo(long index, double value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] += value;
        }

        @Override
        public void setAll(LongToDoubleFunction gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << PAGE_SHIFT;
                Arrays.setAll(pages[i], j -> gen.applyAsDouble(t + j));
            }
        }

        @Override
        public void fill(double value) {
            for (double[] page : pages) {
                Arrays.fill(page, value);
            }
        }

        @Override
        public void copyTo(HugeDoubleArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeDoubleArray) {
                SingleHugeDoubleArray dst = (SingleHugeDoubleArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (double[] page : pages) {
                    int toCopy = Math.min(remaining, page.length);
                    if (toCopy == 0) {
                        break;
                    }
                    System.arraycopy(page, 0, dst.page, start, toCopy);
                    start += toCopy;
                    remaining -= toCopy;
                }
                Arrays.fill(dst.page, start, dst.size, 0D);
            } else if (dest instanceof PagedHugeDoubleArray) {
                PagedHugeDoubleArray dst = (PagedHugeDoubleArray) dest;
                int pageLen = Math.min(pages.length, dst.pages.length);
                int lastPage = pageLen - 1;
                long remaining = length;
                for (int i = 0; i < lastPage; i++) {
                    double[] page = pages[i];
                    double[] dstPage = dst.pages[i];
                    System.arraycopy(page, 0, dstPage, 0, page.length);
                    remaining -= page.length;
                }
                if (remaining > 0L) {
                    System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                    Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, 0D);
                }
                for (int i = pageLen; i < dst.pages.length; i++) {
                    Arrays.fill(dst.pages[i], 0D);
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
        public HugeCursor<double[]> newCursor() {
            return new HugeCursor.PagedCursor<>(size, pages);
        }
    }
}
