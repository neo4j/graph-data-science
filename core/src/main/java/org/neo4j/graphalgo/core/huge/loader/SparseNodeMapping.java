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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

public final class SparseNodeMapping {

    private static final long NOT_FOUND = -1L;

    private static final int PAGE_SHIFT = 12;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);
    private static final long PAGE_SIZE_IN_BYTES = MemoryUsage.sizeOfLongArray(PAGE_SIZE);

    private final long capacity;
    private final long[][] pages;

    private SparseNodeMapping(long capacity, long[][] pages) {
        this.capacity = capacity;
        this.pages = pages;
    }

    /**
     * @param size highest id that we need to represent
     *             (equals size in {@link SparseNodeMapping.Builder#create(long, AllocationTracker)})
     * @param maxEntries number of identifiers we need to store
     */
    public static MemoryRange memoryEstimation(long size, long maxEntries) {
        assert(maxEntries <= size);
        int numPagesForSize = PageUtil.numPagesFor(size, PAGE_SHIFT, (int) PAGE_MASK);
        int numPagesForMaxEntriesBestCase = PageUtil.numPagesFor(maxEntries, PAGE_SHIFT, (int) PAGE_MASK);

        // Worst-case distribution assumes at least one entry per page
        final long maxEntriesForWorstCase = Math.min(size, maxEntries * PAGE_SIZE);
        int numPagesForMaxEntriesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, PAGE_SHIFT, (int) PAGE_MASK);

        long classSize = MemoryUsage.sizeOfInstance(SparseNodeMapping.class);
        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);
        long minRequirements = numPagesForMaxEntriesBestCase * PAGE_SIZE_IN_BYTES;
        long maxRequirements = numPagesForMaxEntriesWorstCase * PAGE_SIZE_IN_BYTES;
        return MemoryRange.of(classSize + pagesSize).add(MemoryRange.of(minRequirements, maxRequirements));
    }

    public long get(long index) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        long[] page = pages[pageIndex];
        if (page != null) {
            final int indexInPage = indexInPage(index);
            return page[indexInPage];
        }
        return NOT_FOUND;
    }

    public boolean contains(long index) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        long[] page = pages[pageIndex];
        if (page != null) {
            final int indexInPage = indexInPage(index);
            return page[indexInPage] != NOT_FOUND;
        }
        return false;
    }

    private static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }

    public static final class Builder {
        private final long capacity;
        private final AtomicReferenceArray<long[]> pages;
        private final AllocationTracker tracker;
        private final ReentrantLock newPageLock;

        public static Builder create(
                long size,
                AllocationTracker tracker) {
            int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, (int) PAGE_MASK);
            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
            AtomicReferenceArray<long[]> pages = new AtomicReferenceArray<>(numPages);
            tracker.add(MemoryUsage.sizeOfObjectArray(numPages));
            return new Builder(capacity, pages, tracker);
        }

        private Builder(long capacity, AtomicReferenceArray<long[]> pages, AllocationTracker tracker) {
            this.capacity = capacity;
            this.pages = pages;
            this.tracker = tracker;
            newPageLock = new ReentrantLock(true);
        }

        public void set(long index, long value) {
            assert index < capacity;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            long[] page = pages.get(pageIndex);
            if (page == null) {
                page = allocateNewPage(pageIndex);
            }
            page[indexInPage] = value;
        }

        public SparseNodeMapping build() {
            int numPages = this.pages.length();
            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
            long[][] pages = new long[numPages][];
            Arrays.setAll(pages, this.pages::get);
            return new SparseNodeMapping(capacity, pages);
        }

        private long[] allocateNewPage(int pageIndex) {
            // TODO: CAS instead of locks? might create `(num_threads - 1) * long[PAGE_SIZE]` that are thrown away immediately
            newPageLock.lock();
            try {
                long[] page = pages.get(pageIndex);
                if (page != null) {
                    return page;
                }
                tracker.add(PAGE_SIZE_IN_BYTES);
                page = new long[PAGE_SIZE];
                Arrays.fill(page, -1L);
                pages.set(pageIndex, page);
                return page;
            } finally {
                newPageLock.unlock();
            }
        }
    }
}
