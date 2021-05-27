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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.oversizeInt;


public final class HugeSparseLongArray {

    private static final long NOT_FOUND = NodeMapping.NOT_FOUND;

    private static final int PAGE_SHIFT = 12;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long PAGE_SIZE_IN_BYTES = MemoryUsage.sizeOfLongArray(PAGE_SIZE);

    private final long capacity;
    private final long[][] pages;
    private final long defaultValue;

    private HugeSparseLongArray(long capacity, long[][] pages, long defaultValue) {
        this.capacity = capacity;
        this.pages = pages;
        this.defaultValue = defaultValue;
    }

    public static Builder builder(long size, AllocationTracker tracker) {
        return builder(size, NOT_FOUND, tracker);
    }

    public static Builder builder(long size, long defaultValue, AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, PAGE_MASK);
        long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
        AtomicReferenceArray<long[]> pages = new AtomicReferenceArray<>(numPages);
        tracker.add(MemoryUsage.sizeOfObjectArray(numPages));
        return new Builder(capacity, pages, defaultValue, tracker);
    }

    /**
     * @param maxId highest id that we need to represent
     *             (equals size in {@link HugeSparseLongArray.Builder#builder(long, AllocationTracker)})
     * @param maxEntries number of identifiers we need to store
     */
    public static MemoryRange memoryEstimation(long maxId, long maxEntries) {
        assert(maxEntries <= maxId);
        int numPagesForSize = PageUtil.numPagesFor(maxId, PAGE_SHIFT, PAGE_MASK);
        int numPagesForMaxEntriesBestCase = PageUtil.numPagesFor(maxEntries, PAGE_SHIFT, PAGE_MASK);

        // Worst-case distribution assumes at least one entry per page
        final long maxEntriesForWorstCase = Math.min(maxId, maxEntries * PAGE_SIZE);
        int numPagesForMaxEntriesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, PAGE_SHIFT, PAGE_MASK);

        long classSize = MemoryUsage.sizeOfInstance(HugeSparseLongArray.class);
        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);
        long minRequirements = numPagesForMaxEntriesBestCase * PAGE_SIZE_IN_BYTES;
        long maxRequirements = numPagesForMaxEntriesWorstCase * PAGE_SIZE_IN_BYTES;
        return MemoryRange.of(classSize + pagesSize).add(MemoryRange.of(minRequirements, maxRequirements));
    }

    public long getCapacity() {
        return capacity;
    }

    public long get(long index) {
        final int pageIndex = pageIndex(index);
        if (pageIndex < pages.length) {
            long[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage(index)];
            }
        }
        return defaultValue;
    }

    public boolean contains(long index) {
        final int pageIndex = pageIndex(index);
        if (pageIndex < pages.length) {
            long[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage(index)] != defaultValue;
            }
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
        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        private final long capacity;
        private final long defaultValue;

        private final AtomicReferenceArray<long[]> pages;
        private final AllocationTracker tracker;
        private final ReentrantLock newPageLock;

        private Builder(long capacity, AtomicReferenceArray<long[]> pages, long defaultValue, AllocationTracker tracker) {
            this.capacity = capacity;
            this.pages = pages;
            this.tracker = tracker;
            this.defaultValue = defaultValue;
            this.newPageLock = new ReentrantLock(true);
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

        /**
         * @return true iff the value was absent and was added, false if there already was a value at this position.
         */
        public boolean setIfAbsent(long index, long value) {
            assert index < capacity;
            final int pageIndex = pageIndex(index);
            final int indexInPage = indexInPage(index);
            long[] page = pages.get(pageIndex);
            if (page == null) {
                page = allocateNewPage(pageIndex);
            }

            long storedValue = (long) ARRAY_HANDLE.compareAndExchange(
                page,
                indexInPage,
                defaultValue,
                value
            );
            return storedValue == defaultValue;
        }

        public HugeSparseLongArray build() {
            int numPages = this.pages.length();
            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
            long[][] pages = new long[numPages][];
            Arrays.setAll(pages, this.pages::get);
            return new HugeSparseLongArray(capacity, pages, defaultValue);
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
                if (defaultValue != 0L) {
                    Arrays.fill(page, defaultValue);
                }
                pages.set(pageIndex, page);
                return page;
            } finally {
                newPageLock.unlock();
            }
        }
    }

    public static final class GrowingBuilder {
        private final AllocationTracker tracker;
        private final ReentrantLock newPageLock;
        private final long defaultValue;

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        private AtomicReferenceArray<long[]> pages;

        public static GrowingBuilder create(AllocationTracker tracker) {
            return create(NOT_FOUND, tracker);
        }

        public static GrowingBuilder create(long defaultValue, AllocationTracker tracker) {
            AtomicReferenceArray<long[]> pages = new AtomicReferenceArray<>(0);
            return new GrowingBuilder(pages, defaultValue, tracker);
        }

        private GrowingBuilder(AtomicReferenceArray<long[]> pages, long defaultValue, AllocationTracker tracker) {
            this.pages = pages;
            this.tracker = tracker;
            this.defaultValue = defaultValue;
            this.newPageLock = new ReentrantLock(true);
        }

        public void set(long index, long value) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            ARRAY_HANDLE.setVolatile(getPage(pageIndex), indexInPage, value);
        }

        public void addTo(long index, long value) {
            int pageIndex = pageIndex(index);
            int indexInPage = indexInPage(index);
            long[] page = getPage(pageIndex);

            long expectedCurrentValue = (long) ARRAY_HANDLE.getVolatile(page, indexInPage);

            while (true) {
                var newValueToStore = expectedCurrentValue + value;
                long actualCurrentValue = (long) ARRAY_HANDLE.compareAndExchange(
                    page,
                    indexInPage,
                    expectedCurrentValue,
                    newValueToStore
                );
                if (actualCurrentValue == expectedCurrentValue) {
                    return ;
                }
                expectedCurrentValue = actualCurrentValue;
            }
        }

        public HugeSparseLongArray build() {
            int numPages = this.pages.length();
            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
            long[][] pages = new long[numPages][];
            Arrays.setAll(pages, this.pages::get);
            return new HugeSparseLongArray(capacity, pages, defaultValue);
        }

        private long[] getPage(int pageIndex) {
            if (pageIndex >= pages.length()) {
                grow(pageIndex + 1);
            }

            long[] page = pages.get(pageIndex);
            if (page == null) {
                page = allocateNewPage(pageIndex);
            }
            return page;
        }

        private void grow(int newSize) {
            newPageLock.lock();
            try {
                if (newSize <= pages.length()) {
                    return;
                }

                AtomicReferenceArray<long[]> newPages = new AtomicReferenceArray<>(oversizeInt(newSize, MemoryUsage.BYTES_OBJECT_REF));
                for (int pageIndex = 0; pageIndex < pages.length(); pageIndex++) {
                    long[] page = pages.get(pageIndex);
                    if (page != null) {
                        newPages.set(pageIndex, page);
                    }
                }
                this.pages = newPages;
            } finally {
                newPageLock.unlock();
            }
        }

        private long[] allocateNewPage(int pageIndex) {
            newPageLock.lock();
            try {
                long[] page = pages.get(pageIndex);
                if (page != null) {
                    return page;
                }
                tracker.add(PAGE_SIZE_IN_BYTES);
                page = new long[PAGE_SIZE];
                if (defaultValue != 0L) {
                    Arrays.fill(page, defaultValue);
                }
                pages.set(pageIndex, page);
                return page;
            } finally {
                newPageLock.unlock();
            }
        }
    }
}
