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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArrayElements;

/**
 * "Bump" refers to the implementation in that there is a local allocator that is able to do a fast-path allocation
 * by just bumping a pointer value. The name follows the description of the TLAB-allocation from the JVM.
 * https://shipilev.net/jvm/anatomy-quarks/4-tlab-allocation
 */
public final class BumpAllocator<PAGE> {

    public static final int PAGE_SHIFT = 18;
    public static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    public static final long PAGE_MASK = PAGE_SIZE - 1;

    private static final int NO_SKIP = -1;

    private static final VarHandle PAGES;
    private static final VarHandle ALLOCATED_PAGES;

    @SuppressWarnings({"FieldMayBeFinal", "unused"})
    private volatile int allocatedPages;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile PAGE[] pages;

    private final Factory<PAGE> pageFactory;
    private final ReentrantLock growLock;
    private final AllocationTracker tracker;

    BumpAllocator(AllocationTracker tracker, Factory<PAGE> pageFactory) {
        this.pageFactory = pageFactory;
        this.tracker = tracker;
        this.growLock = new ReentrantLock(true);
        this.pages = pageFactory.newEmptyPages();
        tracker.add(sizeOfObjectArray(0));
    }

    static {
        try {
            PAGES = MethodHandles.lookup().findVarHandle(BumpAllocator.class, "pages", Object[].class);
            ALLOCATED_PAGES = MethodHandles.lookup().findVarHandle(BumpAllocator.class, "allocatedPages", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public LocalAllocator<PAGE> newLocalAllocator() {
        return new LocalAllocator<>(this, false);
    }

    public LocalAllocator<PAGE> newPrefetchingOneBasedLocalAllocator() {
        return new LocalAllocator<>(this, true);
    }

    public PAGE[] intoPages() {
        return pages;
    }

    private long insertDefaultSizedPage() {
        int pageIndex = (int) ALLOCATED_PAGES.getAndAdd(this, 1);
        grow(pageIndex + 1, NO_SKIP);
        return PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
    }

    private long insertExistingPage(PAGE page) {
        int pageIndex = (int) ALLOCATED_PAGES.getAndAdd(this, 1);
        grow(pageIndex + 1, pageIndex);

        // We already increased `pages` for the oversize page in `grow()`.
        // We need to insert the new page at the right position and
        // remove the previously tracked memory. This has to happen
        // within the grow lock to avoid the `pages` reference to be
        // overwritten by another thread during `grow()`.
        growLock.lock();
        try {
            tracker.add(pageFactory.memorySizeOfPage(page));
            var pages = this.pages;
            if (pages[pageIndex] != null) {
                tracker.remove(pageFactory.memorySizeOfPage(PAGE_SIZE));
            }
            pages[pageIndex] = page;
        } finally {
            growLock.unlock();
        }
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
        return newNumPages <= this.pages.length;
    }

    /**
     * Grows and re-assigns the {@code pages} representing the Adjacency List.
     *
     * This method is not thread-safe.
     * Callers need to acquire the {@code growLock} before entering the method.
     */
    private void setPages(int newNumPages, int skipPage) {
        PAGE[] currentPages = this.pages;
        tracker.add(sizeOfObjectArrayElements(newNumPages - currentPages.length));

        PAGE[] newPages = Arrays.copyOf(currentPages, newNumPages);

        for (int i = currentPages.length; i < newNumPages; i++) {
            // Create new page for default sized pages
            if (i != skipPage) {
                tracker.add(pageFactory.memorySizeOfPage(PAGE_SIZE));
                newPages[i] = pageFactory.newPage(PAGE_SIZE);
            }
        }
        PAGES.set(this, newPages);
    }

    public interface Factory<PAGE> {
        PAGE[] newEmptyPages();

        PAGE newPage(int length);

        PAGE copyOfPage(PAGE page, int length);

        int lengthOfPage(PAGE page);

        default long memorySizeOfPage(PAGE page) {
            return memorySizeOfPage(lengthOfPage(page));
        }

        long memorySizeOfPage(int length);
    }

    public static final class LocalAllocator<PAGE> {

        private final BumpAllocator<PAGE> globalAllocator;

        private long top;

        private PAGE page;
        private int offset;

        private LocalAllocator(BumpAllocator<PAGE> globalAllocator, boolean prefetchAndBumpZero) {
            this.globalAllocator = globalAllocator;

            this.offset = PAGE_SIZE;
            if (prefetchAndBumpZero) {
                top = prefetchAllocate();
                if (top == 0L) {
                    ++top;
                    ++offset;
                }
            }
        }

        /**
         * Inserts slice into the allocator, returns global address
         */
        public long insert(PAGE targets, int length) {
            // targetLength is the length of the array that is provided ({@code == targets.length}).
            // This value can be greater than `length` if the provided array is some sort of a buffer.
            // We need this to determine if we need to make a slice-copy of the targets array or not.
            var targetLength = globalAllocator.pageFactory.lengthOfPage(targets);
            return insertData(targets, Math.min(length, targetLength), top, targetLength);
        }

        private long insertData(PAGE targets, int length, long address, int targetsLength) {
            int maxOffset = PAGE_SIZE - length;
            if (maxOffset >= offset) {
                doAllocate(targets, length);
                return address;
            }
            return slowPathAllocate(targets, length, maxOffset, address, targetsLength);
        }

        private long slowPathAllocate(PAGE targets, int length, int maxOffset, long address, int targetsLength) {
            if (maxOffset < 0) {
                return oversizingAllocate(targets, length, targetsLength);
            }
            return prefetchAllocate(targets, length);
        }

        /**
         * We are faking a valid page by over-allocating a single page to be large enough to hold all data
         * Since we are storing all degrees into a single page and thus never have to switch pages
         * and keep the offsets as if this page would be of the correct size, we might just get by.
         */
        private long oversizingAllocate(PAGE targets, int length, int targetsLength) {
            if (length < targetsLength) {
                // need to create a smaller slice
                targets = globalAllocator.pageFactory.copyOfPage(targets, length);
            }
            return globalAllocator.insertExistingPage(targets);
        }

        private long prefetchAllocate(PAGE targets, int length) {
            long address = prefetchAllocate();
            doAllocate(targets, length);
            return address;
        }

        private long prefetchAllocate() {
            long address = top = globalAllocator.insertDefaultSizedPage();
            assert PageUtil.indexInPage(address, PAGE_MASK) == 0;
            var currentPageIndex = PageUtil.pageIndex(address, PAGE_SHIFT);
            this.page = globalAllocator.pages[currentPageIndex];
            this.offset = 0;
            return address;
        }

        @SuppressWarnings("SuspiciousSystemArraycopy")
        private void doAllocate(PAGE targets, int length) {
            System.arraycopy(targets, 0, page, offset, length);
            offset += length;
            top += length;
        }
    }
}
