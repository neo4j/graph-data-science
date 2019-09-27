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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.huge.AdjacencyList.PAGE_MASK;
import static org.neo4j.graphalgo.core.huge.AdjacencyList.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.huge.AdjacencyList.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfByteArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArrayElements;


final class AdjacencyListBuilder {

    private static final long MAX_SIZE = 1L << (Integer.SIZE - 1 + PAGE_SHIFT);
    private static final long PAGE_SIZE_IN_BYTES = sizeOfByteArray(PAGE_SIZE);
    private static final int PREFETCH_PAGES = 1;
    private static final long PREFETCH_ELEMENTS = ((long) PREFETCH_PAGES) << PAGE_SHIFT;

    private final AllocationTracker tracker;
    private final ReentrantLock growLock;

    private final AtomicLong allocIdx;
    private final AtomicLong size;
    private final AtomicLong capacity;

    private byte[][] pages;

    static AdjacencyListBuilder newBuilder(AllocationTracker tracker) {
        return new AdjacencyListBuilder(tracker);
    }

    private AdjacencyListBuilder(AllocationTracker tracker) {
        this.tracker = tracker;
        growLock = new ReentrantLock(true);
        size = new AtomicLong();
        capacity = new AtomicLong();
        allocIdx = new AtomicLong();
        pages = new byte[0][];
        tracker.add(sizeOfObjectArray(0));
    }

    Allocator newAllocator() {
        return new Allocator(this);
    }

    public AdjacencyList build() {
        return new AdjacencyList(pages);
    }

    private long allocateNewPages(Allocator into) {
        long intoIndex = allocIdx.getAndAdd(PREFETCH_ELEMENTS);
        grow(intoIndex + PREFETCH_ELEMENTS);
        into.setNewPages(pages, intoIndex);
        return intoIndex;
    }

    private long allocatePage(byte[] page, Allocator into) {
        long intoIndex = allocIdx.getAndAdd(PAGE_SIZE);
        int pageIndex = PageUtil.pageIndex(intoIndex, PAGE_SHIFT);
        grow(intoIndex + PAGE_SIZE, pageIndex, page);
        tracker.add(sizeOfByteArray(page.length));
        into.insertPage(page);
        return intoIndex;
    }

    private void grow(final long newSize) {
        grow(newSize, -1, null);
    }

    private void grow(final long newSize, final int skipPage, byte[] page) {
        assert newSize <= MAX_SIZE;
        boolean didSetSize = tryGrowSize(newSize);
        if (didSetSize) {
            return;
        }
        growLock.lock();
        try {
            didSetSize = tryGrowSize(newSize);
            if (didSetSize) {
                return;
            }
            int newNumPages = PageUtil.numPagesFor(newSize, PAGE_SHIFT, PAGE_MASK);
            long newCap = PageUtil.capacityFor(newNumPages, PAGE_SHIFT);
            setPages(newNumPages, this.pages.length, skipPage, page);
            capacity.set(newCap);
            size.set(newSize);
        } finally {
            growLock.unlock();
        }
    }

    private boolean tryGrowSize(long newSize) {
        long cap = capacity.get();
        if (newSize > cap) {
            return false;
        }
        setSize(newSize);
        return true;
    }

    private void setSize(long newSize) {
        long size;
        do {
            size = this.size.get();
        } while (size < newSize && !this.size.compareAndSet(size, newSize));
    }

    private void setPages(int newNumPages, int currentNumPages, int skipPage, byte[] page) {
        assert skipPage == -1L || page != null : "Trying to insert null page at skipPage index (" + skipPage +")";

        int newPages = newNumPages - currentNumPages;
        if (newPages > 0) {
            AllocationTracker tracker = this.tracker;
            tracker.add(sizeOfObjectArrayElements(newPages));
            byte[][] pages = Arrays.copyOf(this.pages, newNumPages);
            for (int i = currentNumPages; i < newNumPages; i++) {
                if (i != skipPage) {
                    tracker.add(PAGE_SIZE_IN_BYTES);
                    pages[i] = new byte[PAGE_SIZE];
                } else {
                    pages[i] = page;
                }
            }
            this.pages = pages;
        }
    }

    static final class Allocator {

        private final AdjacencyListBuilder builder;

        private long top;

        private byte[][] pages;
        private int prevOffset;
        private int toPageIndex;
        private int currentPageIndex;

        public byte[] page;
        public int offset;

        private Allocator(final AdjacencyListBuilder builder) {
            this.builder = builder;
            prevOffset = -1;
        }

        void prepare() {
            top = builder.allocateNewPages(this);
            if (top == 0L) {
                ++top;
                ++offset;
            }
        }

        long allocate(int size) {
            return localAllocate(size, top);
        }

        private long localAllocate(int size, long address) {
            int maxOffset = PAGE_SIZE - size;
            if (maxOffset >= offset) {
                top += size;
                return address;
            }
            return majorAllocate(size, maxOffset, address);
        }

        private long majorAllocate(int size, int maxOffset, long address) {
            if (maxOffset < 0) {
                return oversizingAllocate(size);
            }
            if (reset() && maxOffset >= offset) {
                top += size;
                return address;
            }
            int waste = PAGE_SIZE - offset;
            address = top += waste;
            if (next()) {
                // TODO: store and reuse fragments
                // branch: huge-alloc-fragmentation-recycle
                top += size;
                return address;
            }
            return prefetchAllocate(size);
        }

        /**
         * We are faking a valid page by over-allocating a single page to be large enough to hold all data
         * Since we are storing all degrees into a single page and thus never have to switch pages
         * and keep the offsets as if this page would be of the correct size, we might just get by.
         */
        private long oversizingAllocate(int size) {
            byte[] largePage = new byte[size];
            return builder.allocatePage(largePage, this);
        }

        private long prefetchAllocate(int size) {
            long address = top = builder.allocateNewPages(this);
            top += size;
            return address;
        }

        private boolean reset() {
            if (prevOffset != -1) {
                page = pages[currentPageIndex];
                offset = prevOffset;
                prevOffset = -1;
                return true;
            }
            return false;
        }

        private boolean next() {
            if (++currentPageIndex <= toPageIndex) {
                page = pages[currentPageIndex];
                offset = 0;
                return true;
            }
            page = null;
            return false;
        }

        private void setNewPages(byte[][] pages, long fromIndex) {
            assert PageUtil.indexInPage(fromIndex, PAGE_MASK) == 0;
            this.pages = pages;
            currentPageIndex = PageUtil.pageIndex(fromIndex, PAGE_SHIFT);
            toPageIndex = currentPageIndex + PREFETCH_PAGES - 1;
            page = pages[currentPageIndex];
            offset = 0;
        }

        private void insertPage(byte[] page) {
            if (prevOffset == -1) {
                prevOffset = offset;
            }
            this.page = page;
            offset = 0;
        }
    }
}
