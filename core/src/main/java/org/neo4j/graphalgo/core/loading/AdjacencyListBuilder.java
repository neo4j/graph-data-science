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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.huge.AdjacencyList.PAGE_MASK;
import static org.neo4j.graphalgo.core.huge.AdjacencyList.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.huge.AdjacencyList.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfByteArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArrayElements;

final class AdjacencyListBuilder {

    private static final long PAGE_SIZE_IN_BYTES = sizeOfByteArray(PAGE_SIZE);
    private static final AtomicReferenceFieldUpdater<AdjacencyListBuilder, byte[][]> PAGES_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(AdjacencyListBuilder.class, byte[][].class, "pages");

    private final AllocationTracker tracker;
    private final ReentrantLock growLock;
    private final AtomicInteger allocatedPages;

    @SuppressWarnings("FieldMayBeFinal")
    private volatile byte[][] pages;

    static AdjacencyListBuilder newBuilder(AllocationTracker tracker) {
        return new AdjacencyListBuilder(tracker);
    }

    private AdjacencyListBuilder(AllocationTracker tracker) {
        this.tracker = tracker;
        growLock = new ReentrantLock(true);
        allocatedPages = new AtomicInteger();
        pages = new byte[0][];
        tracker.add(sizeOfObjectArray(0));
    }

    Allocator newAllocator() {
        return new Allocator(this);
    }

    public AdjacencyList build() {
        return new AdjacencyList(pages);
    }

    private long insertDefaultSizedPage(Allocator into) {
        int pageIndex = allocatedPages.getAndIncrement();
        grow(pageIndex + 1, -1);
        long intoIndex = PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
        into.setNewPages(pages, intoIndex);
        return intoIndex;
    }

    private long insertOversizedPage(byte[] page, Allocator into) {
        int pageIndex = allocatedPages.getAndIncrement();
        grow(pageIndex + 1, pageIndex);

        // We already increased `pages` for the oversize page in `grow()`.
        // We need to insert the new page at the right position and
        // remove the previously tracked memory. This has to happen
        // within the grow lock to avoid the `pages` reference to be
        // overwritten by another thread during `grow()`.
        growLock.lock();
        try {
            tracker.add(sizeOfByteArray(page.length));
            if (PAGES_UPDATER.get(this)[pageIndex] != null) {
                tracker.remove(PAGE_SIZE_IN_BYTES);
            }
            PAGES_UPDATER.get(this)[pageIndex] = page;
        } finally {
            growLock.unlock();
        }
        into.insertPage(page);

        return PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
    }

    private void grow(int newNumPages, int skipPage) {
        if (capacityLeft(newNumPages)) {
            return;
        }
        growLock.lock();
        try {
            if (capacityLeft(newNumPages)) {
                return;
            }
            setPages(newNumPages, skipPage);
        } finally {
            growLock.unlock();
        }
    }

    private boolean capacityLeft(long newNumPages) {
        return newNumPages <= PAGES_UPDATER.get(this).length;
    }

    private void setPages(int newNumPages, int skipPage) {
        byte[][] currentPages = PAGES_UPDATER.get(this);
        tracker.add(sizeOfObjectArrayElements(newNumPages - currentPages.length));

        byte[][] newPages = Arrays.copyOf(currentPages, newNumPages);

        for (int i = currentPages.length; i < newNumPages; i++) {
            // Create new page for default sized pages
            if (i != skipPage) {
                tracker.add(PAGE_SIZE_IN_BYTES);
                newPages[i] = new byte[PAGE_SIZE];
            }
        }
        PAGES_UPDATER.set(this, newPages);
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

        private Allocator(AdjacencyListBuilder builder) {
            this.builder = builder;
            prevOffset = -1;
        }

        void prepare() {
            top = builder.insertDefaultSizedPage(this);
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
            return builder.insertOversizedPage(largePage, this);
        }

        private long prefetchAllocate(int size) {
            long address = top = builder.insertDefaultSizedPage(this);
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
            toPageIndex = currentPageIndex;
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
