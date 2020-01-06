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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class PagedDataStructure<T> {

    final int pageSize;
    final int pageShift;
    final int pageMask;
    private final long maxSupportedSize;

    volatile T[] pages;

    private final AtomicLong size = new PaddedAtomicLong();
    private final AtomicLong capacity = new PaddedAtomicLong();
    private final ReentrantLock growLock = new ReentrantLock(true);

    private final PageAllocator<T> allocator;

    PagedDataStructure(long size, PageAllocator<T> allocator) {
        pageSize = allocator.pageSize();
        pageShift = Integer.numberOfTrailingZeros(pageSize);
        pageMask = pageSize - 1;

        final int maxIndexShift = Integer.SIZE - 1 + pageShift;
        maxSupportedSize = 1L << maxIndexShift;
        assert size <= maxSupportedSize;
        this.size.set(size);

        this.allocator = allocator;
        pages = allocator.emptyPages();
        setPages(numPages(size));
    }

    PagedDataStructure(long size, T[] pages, PageAllocator<T> allocator) {
        pageSize = allocator.pageSize();
        pageShift = Integer.numberOfTrailingZeros(pageSize);
        pageMask = pageSize - 1;

        if (numPages(size) != pages.length) {
            throw new IllegalArgumentException(String.format(
                    "The capacity of [%d] would require [%d] pages, but [%d] were provided",
                    size,
                    numPages(size),
                    pages.length));
        }

        final int maxIndexShift = Integer.SIZE - 1 + pageShift;
        maxSupportedSize = 1L << maxIndexShift;

        this.allocator = allocator;
        this.pages = pages;
        this.capacity.set(capacityFor(pages.length));
        this.size.set(size);
    }

    /**
     * Return the size of this data structure. Indices up to {@code size}
     * have been filled with data.
     */
    public long size() {
        return size.get();
    }

    /**
     * Return the capacity of this data structure. Not all indices up to this value may have
     * sensible data, but it can be safely written up to this index (exclusive).
     */
    public final long capacity() {
        return capacity.get();
    }

    public long release() {
        size.set(0);
        long freed = allocator.estimateMemoryUsage(capacity.getAndSet(0));
        pages = null;
        return freed;
    }

    protected int numPages(long capacity) {
        return PageUtil.numPagesFor(capacity, pageShift, pageMask);
    }

    final long capacityFor(int numPages) {
        return ((long) numPages) << pageShift;
    }

    final int pageIndex(long index) {
        return (int) (index >>> pageShift);
    }

    final int indexInPage(long index) {
        return (int) (index & pageMask);
    }

    /**
     * Grows the page structure to the new size. The existing content will be preserved.
     * If the current size is large enough, this is no-op and no downsizing is happening.
     */
    final void grow(final long newSize) {
        grow(newSize, -1);
    }

    /**
     * Grows the page structure to the new size. The existing content will be preserved.
     * If the current size is large enough, this is no-op and no downsizing is happening.
     */
    final void grow(final long newSize, final int skipPage) {
        assert newSize <= maxSupportedSize;
        long cap = capacity.get();
        if (cap >= newSize) {
            growSize(newSize);
            return;
        }
        growLock.lock();
        try {
            cap = capacity.get();
            if (cap >= newSize) {
                growSize(newSize);
                return;
            }
            setPages(numPages(newSize), this.pages.length, skipPage);
            growSize(newSize);
        } finally {
            growLock.unlock();
        }
    }

    private void growSize(final long newSize) {
        long size;
        do {
            size = this.size.get();
        } while (size < newSize && !this.size.compareAndSet(size, newSize));
    }

    private void setPages(int numPages) {
        if (numPages > 0) {
            setPages(numPages, 0, -1);
        }
    }

    private void setPages(int numPages, int currentNumPages, int skipPage) {
        T[] pages = Arrays.copyOf(this.pages, numPages);
        for (int i = currentNumPages; i < numPages; i++) {
            if (i != skipPage) {
                pages[i] = allocateNewPage();
            }
        }
        this.pages = pages;
        this.capacity.set(capacityFor(numPages));
    }

    T allocateNewPage() {
        return allocator.newPage();
    }
}
