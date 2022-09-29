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

import org.eclipse.collections.api.block.function.primitive.LongToByteFunction;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.mem.HugeArrays;

import java.util.Arrays;
import java.util.function.LongFunction;

import static org.neo4j.gds.mem.HugeArrays.PAGE_SHIFT;
import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;
import static org.neo4j.gds.mem.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.gds.mem.HugeArrays.indexInPage;
import static org.neo4j.gds.mem.HugeArrays.numberOfPages;
import static org.neo4j.gds.mem.HugeArrays.pageIndex;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfByteArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;

/**
 * A long-indexable version of a primitive byte array ({@code byte[]}) that can contain more than 2 bn. elements.
 * <p>
 * It is implemented by paging of smaller byte-arrays ({@code byte[][]}) to support approx. 32k bn. elements.
 * If the provided size is small enough, an optimized view of a single {@code byte[]} might be used.
 *
 * <ul>
 * <li>The array is of a fixed size and cannot grow or shrink dynamically.</li>
 * <li>The array is not optimized for sparseness and has a large memory overhead if the values written to it are very sparse (see {@link org.neo4j.gds.collections.HugeSparseByteArray} for a different implementation that can profit from sparse data).</li>
 * <li>The array does not support default values and returns the same default for unset values that a regular {@code byte[]} does ({@code 0}).</li>
 * </ul>
 *
 * <p><em>Basic Usage</em></p>
 * <pre>
 * {@code}
 * AllocationTracker allocationTracker = ...;
 * long arraySize = 42L;
 * HugeByteArray array = HugeByteArray.newArray(arraySize, allocationTracker);
 * array.set(13L, 37);
 * byte value = array.get(13L);
 * // value = 37
 * {@code}
 * </pre>
 */
public abstract class HugeByteArray extends HugeArray<byte[], Byte, HugeByteArray> {

    /**
     * @return the byte value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract byte get(long index);

    public abstract byte getAndAdd(long index, byte delta);

    /**
     * Sets the byte value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void set(long index, byte value);

    /**
     * Computes the bit-wise OR ({@code |}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x | 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void or(long index, byte value);

    /**
     * Computes the bit-wise AND ({@code &}) of the existing value and the provided value at the given index.
     * If there was no previous value, the final result is set to the 0 ({@code x & 0 == 0}).
     *
     * @return the now current value after the operation
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract byte and(long index, byte value);

    /**
     * Adds ({@code +}) the existing value and the provided value at the given index and stored the result into the given index.
     * If there was no previous value, the final result is set to the provided value ({@code x + 0 == x}).
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    public abstract void addTo(long index, byte value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(int[], java.util.function.IntUnaryOperator)} but for bytes.
     */
    public abstract void setAll(LongToByteFunction gen);

    /**
     * Assigns the specified byte value to each element.
     * <p>
     * The behavior is identical to {@link java.util.Arrays#fill(byte[], byte)}.
     */
    public abstract void fill(byte value);

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
    public abstract HugeCursor<byte[]> newCursor();

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void copyTo(final HugeByteArray dest, final long length);

    /**
     * {@inheritDoc}
     */
    @Override
    final Byte boxedGet(final long index) {
        return get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSet(final long index, final Byte value) {
        set(index, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedSetAll(final LongFunction<Byte> gen) {
        setAll(gen::apply);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final void boxedFill(final Byte value) {
        fill(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] toArray() {
        return dumpToArray(byte[].class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final HugeByteArray copyOf(final long newLength) {
        HugeByteArray copy = HugeByteArray.newArray(newLength);
        this.copyTo(copy, newLength);
        return copy;
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
                return HugeByteArray.this.size();
            }
        };
    }

    /**
     * Creates a new array of the given size.
     */
    public static HugeByteArray newArray(long size) {
        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return SingleHugeByteArray.of(size);
        }
        return PagedHugeByteArray.of(size);
    }

    public static HugeByteArray of(final byte... values) {
        return new SingleHugeByteArray(values.length, values);
    }

    public static long memoryEstimation(long size) {
        assert size >= 0;

        if (size <= HugeArrays.MAX_ARRAY_LENGTH) {
            return sizeOfInstance(SingleHugeByteArray.class) + sizeOfByteArray((int) size);
        }
        long sizeOfInstance = sizeOfInstance(PagedHugeByteArray.class);

        int numPages = numberOfPages(size);

        long memoryUsed = sizeOfObjectArray(numPages);
        long pageBytes = sizeOfByteArray(PAGE_SIZE);
        memoryUsed += (numPages - 1) * pageBytes;
        int lastPageSize = exclusiveIndexOfPage(size);

        return sizeOfInstance + memoryUsed + sizeOfByteArray(lastPageSize);
    }

    /* test-only */
    static HugeByteArray newPagedArray(long size) {
        return PagedHugeByteArray.of(size);
    }

    /* test-only */
    static HugeByteArray newSingleArray(int size) {
        return SingleHugeByteArray.of(size);
    }

    private static final class SingleHugeByteArray extends HugeByteArray {

        private static HugeByteArray of(long size) {
            assert size <= HugeArrays.MAX_ARRAY_LENGTH;
            final int intSize = (int) size;
            byte[] page = new byte[intSize];

            return new SingleHugeByteArray(intSize, page);
        }

        private final int size;
        private byte[] page;

        private SingleHugeByteArray(int size, byte[] page) {
            this.size = size;
            this.page = page;
        }

        @Override
        public byte get(long index) {
            assert index < size;
            return page[(int) index];
        }

        @Override
        public byte getAndAdd(long index, byte delta) {
            assert index < size;
            var idx = (int) index;
            var value = page[idx];
            page[idx] += delta;
            return value;
        }

        @Override
        public void set(long index, byte value) {
            assert index < size;
            page[(int) index] = value;
        }

        @Override
        public void or(long index, byte value) {
            assert index < size;
            page[(int) index] |= value;
        }

        @Override
        public byte and(long index, byte value) {
            assert index < size;
            return page[(int) index] &= value;
        }

        @Override
        public void addTo(long index, byte value) {
            assert index < size;
            page[(int) index] += value;
        }


        @Override
        public void setAll(LongToByteFunction gen) {
            for (int i = 0; i < page.length; i++) {
                page[i] = gen.valueOf(i);
            }
        }

        @Override
        public void fill(byte value) {
            Arrays.fill(page, value);
        }

        @Override
        public void copyTo(HugeByteArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeByteArray) {
                SingleHugeByteArray dst = (SingleHugeByteArray) dest;
                System.arraycopy(page, 0, dst.page, 0, (int) length);
                Arrays.fill(dst.page, (int) length, dst.size, (byte) 0);
            } else if (dest instanceof PagedHugeByteArray) {
                PagedHugeByteArray dst = (PagedHugeByteArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (byte[] dstPage : dst.pages) {
                    int toCopy = Math.min(remaining, dstPage.length);
                    if (toCopy == 0) {
                        Arrays.fill(page, (byte) 0);
                    } else {
                        System.arraycopy(page, start, dstPage, 0, toCopy);
                        if (toCopy < dstPage.length) {
                            Arrays.fill(dstPage, toCopy, dstPage.length, (byte) 0);
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
            return sizeOfByteArray(size);
        }

        @Override
        public long release() {
            if (page != null) {
                page = null;
                return sizeOfByteArray(size);
            }
            return 0L;
        }

        @Override
        public HugeCursor<byte[]> newCursor() {
            return new HugeCursor.SinglePageCursor<>(page);
        }

        @Override
        public byte[] toArray() {
            return page;
        }

        @Override
        public String toString() {
            return Arrays.toString(page);
        }
    }

    private static final class PagedHugeByteArray extends HugeByteArray {

        private static HugeByteArray of(long size) {
            int numPages = numberOfPages(size);
            byte[][] pages = new byte[numPages][];

            long memoryUsed = sizeOfObjectArray(numPages);
            final long pageBytes = sizeOfByteArray(PAGE_SIZE);
            for (int i = 0; i < numPages - 1; i++) {
                memoryUsed += pageBytes;
                pages[i] = new byte[PAGE_SIZE];
            }
            final int lastPageSize = exclusiveIndexOfPage(size);
            pages[numPages - 1] = new byte[lastPageSize];
            memoryUsed += sizeOfByteArray(lastPageSize);

            return new PagedHugeByteArray(size, pages, memoryUsed);
        }

        private final long size;
        private byte[][] pages;
        private final long memoryUsed;

        private PagedHugeByteArray(long size, byte[][] pages, long memoryUsed) {
            this.size = size;
            this.pages = pages;
            this.memoryUsed = memoryUsed;
        }

        @Override
        public byte get(long index) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage];
        }

        @Override
        public byte getAndAdd(long index, byte delta) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            var value = pages[pageIndex][indexInPage];
            pages[pageIndex][indexInPage] += delta;
            return value;
        }

        @Override
        public void set(long index, byte value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] = value;
        }

        @Override
        public void or(long index, byte value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] |= value;
        }

        @Override
        public byte and(long index, byte value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            return pages[pageIndex][indexInPage] &= value;
        }

        @Override
        public void addTo(long index, byte value) {
            assert index < size;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            pages[pageIndex][indexInPage] += value;
        }

        @Override
        public void setAll(LongToByteFunction gen) {
            for (int i = 0; i < pages.length; i++) {
                final long t = ((long) i) << PAGE_SHIFT;
                for (int j = 0; j < pages[i].length; j++) {
                    pages[i][j] = gen.valueOf(t + j);
                }
            }
        }

        @Override
        public void fill(byte value) {
            for (byte[] page : pages) {
                Arrays.fill(page, value);
            }
        }

        @Override
        public void copyTo(HugeByteArray dest, long length) {
            if (length > size) {
                length = size;
            }
            if (length > dest.size()) {
                length = dest.size();
            }
            if (dest instanceof SingleHugeByteArray) {
                SingleHugeByteArray dst = (SingleHugeByteArray) dest;
                int start = 0;
                int remaining = (int) length;
                for (byte[] page : pages) {
                    int toCopy = Math.min(remaining, page.length);
                    if (toCopy == 0) {
                        break;
                    }
                    System.arraycopy(page, 0, dst.page, start, toCopy);
                    start += toCopy;
                    remaining -= toCopy;
                }
                Arrays.fill(dst.page, start, dst.size, (byte) 0);
            } else if (dest instanceof PagedHugeByteArray) {
                PagedHugeByteArray dst = (PagedHugeByteArray) dest;
                int pageLen = Math.min(pages.length, dst.pages.length);
                int lastPage = pageLen - 1;
                long remaining = length;
                for (int i = 0; i < lastPage; i++) {
                    byte[] page = pages[i];
                    byte[] dstPage = dst.pages[i];
                    System.arraycopy(page, 0, dstPage, 0, page.length);
                    remaining -= page.length;
                }
                if (remaining > 0L) {
                    System.arraycopy(pages[lastPage], 0, dst.pages[lastPage], 0, (int) remaining);
                    Arrays.fill(dst.pages[lastPage], (int) remaining, dst.pages[lastPage].length, (byte) 0);
                }
                for (int i = pageLen; i < dst.pages.length; i++) {
                    Arrays.fill(dst.pages[i], (byte) 0);
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
        public HugeCursor<byte[]> newCursor() {
            return new HugeCursor.PagedCursor<>(size, pages);
        }
    }
}
