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
package org.neo4j.gds.core.compression.common;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.collections.PageUtil;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

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

    public BumpAllocator(Factory<PAGE> pageFactory) {
        this.pageFactory = pageFactory;
        this.growLock = new ReentrantLock(true);
        this.pages = pageFactory.newEmptyPages();
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
        return new LocalAllocator<>(this);
    }

    public LocalPositionalAllocator<PAGE> newLocalPositionalAllocator() {
        return new LocalPositionalAllocator<>(this);
    }

    public PAGE[] intoPages() {
        return pages;
    }

    private long insertDefaultSizedPage() {
        int pageIndex = (int) ALLOCATED_PAGES.getAndAdd(this, 1);
        grow(pageIndex + 1, NO_SKIP);
        return PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
    }

    private long insertMultiplePages(int uptoPage, @Nullable PAGE page) {
        var currentNumPages = (int) ALLOCATED_PAGES.get(this);
        int newNumPages = uptoPage + 1;
        if (currentNumPages < newNumPages) {
            int pageToSkip = page == null ? NO_SKIP : uptoPage;
            grow(newNumPages, pageToSkip);
        }

        if (page != null) {
            growLock.lock();
            try {
                this.pages[uptoPage] = page;
            } finally {
                growLock.unlock();
            }
        }

        while (currentNumPages < newNumPages) {
            int nextNumPages = (int) ALLOCATED_PAGES.compareAndExchange(this, currentNumPages, newNumPages);
            if (nextNumPages == currentNumPages) {
                currentNumPages = newNumPages;
                break;
            }
            currentNumPages = nextNumPages;
        }

        return PageUtil.capacityFor(currentNumPages, PAGE_SHIFT);
    }

    private long insertExistingPage(@NotNull PAGE page) {
        int pageIndex = (int) ALLOCATED_PAGES.getAndAdd(this, 1);
        grow(pageIndex + 1, pageIndex);

        // We already increased `pages` for the oversize page in `grow()`.
        // We need to insert the new page at the right position and
        // remove the previously tracked memory. This has to happen
        // within the grow lock to avoid the `pages` reference to be
        // overwritten by another thread during `grow()`.
        growLock.lock();
        try {
            this.pages[pageIndex] = page;
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

        PAGE[] newPages = Arrays.copyOf(currentPages, newNumPages);

        for (int i = currentPages.length; i < newNumPages; i++) {
            // Create new page for default sized pages
            if (i != skipPage) {
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
    }

    public static final class LocalAllocator<PAGE> {

        private final BumpAllocator<PAGE> globalAllocator;

        private long top;

        private PAGE page;
        private int offset;

        private LocalAllocator(BumpAllocator<PAGE> globalAllocator) {
            this.globalAllocator = globalAllocator;
            this.offset = PAGE_SIZE;
        }

        /**
         * Inserts slice into the allocator, returns global address
         */
        public long insert(@NotNull PAGE targets, int length) {
            // targetLength is the length of the array that is provided ({@code == targets.length}).
            // This value can be greater than `length` if the provided array is some sort of a buffer.
            // We need this to determine if we need to make a slice-copy of the targets array or not.
            var targetLength = globalAllocator.pageFactory.lengthOfPage(targets);
            return insertData(targets, Math.min(length, targetLength), this.top, targetLength);
        }

        private long insertData(@NotNull PAGE targets, int length, long address, int targetsLength) {
            int maxOffset = PAGE_SIZE - length;
            if (maxOffset >= this.offset) {
                doAllocate(targets, length);
                return address;
            }
            return slowPathAllocate(targets, length, maxOffset, targetsLength);
        }

        private long slowPathAllocate(@NotNull PAGE targets, int length, int maxOffset, int targetsLength) {
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
        private long oversizingAllocate(@NotNull PAGE targets, int length, int targetsLength) {
            if (length < targetsLength) {
                // need to create a smaller slice
                targets = globalAllocator.pageFactory.copyOfPage(targets, length);
            }
            return globalAllocator.insertExistingPage(targets);
        }

        private long prefetchAllocate(@NotNull PAGE targets, int length) {
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
        private void doAllocate(@NotNull PAGE targets, int length) {
            System.arraycopy(targets, 0, this.page, offset, length);
            offset += length;
            top += length;
        }
    }

    public static final class LocalPositionalAllocator<PAGE> {

        private final BumpAllocator<PAGE> globalAllocator;
        private long capacity;

        private LocalPositionalAllocator(BumpAllocator<PAGE> globalAllocator) {
            this.globalAllocator = globalAllocator;
            this.capacity = 0;
        }

        /**
         * Inserts slice into the allocator at the given position
         */
        public void insertAt(long offset, @NotNull PAGE page, int length) {
            // targetLength is the length of the array that is provided ({@code == page.length}).
            // This value can be greater than `length` if the provided array is a buffer of some sort.
            // We need this to determine if we need to make a slice-copy of the page array or not.
            var targetLength = globalAllocator.pageFactory.lengthOfPage(page);
            insertData(offset, page, Math.min(length, targetLength), this.capacity, targetLength);
        }

        private void insertData(long offset, @NotNull PAGE page, int length, long capacity, int targetsLength) {
            @Nullable PAGE pageToInsert = page;
            if (offset + length > capacity) {
                pageToInsert = allocateNewPages(offset, page, length, targetsLength);
            }

            if (pageToInsert != null) {
                int pageId = PageUtil.pageIndex(offset, PAGE_SHIFT);
                int pageOffset = PageUtil.indexInPage(offset, PAGE_MASK);
                PAGE allocatedPage = this.globalAllocator.pages[pageId];

                //noinspection SuspiciousSystemArraycopy
                System.arraycopy(pageToInsert, 0, allocatedPage, pageOffset, length);
            }
        }

        private @Nullable PAGE allocateNewPages(long offset, @NotNull PAGE page, int length, int targetsLength) {
            int pageId = PageUtil.pageIndex(offset, PAGE_SHIFT);

            // We don't want the global allocator to create a new page (while also holding the lock)
            // when we know that we throw it away afterwards. This is the case when we have an oversized page.
            @Nullable PAGE existingPage = null;
            if (length > PAGE_SIZE) {
                if (length < targetsLength) {
                    // We have an oversized page but it contains additional buffer space at the end
                    // We create a new copy of that page that is the exact size to fit all data
                    page = globalAllocator.pageFactory.copyOfPage(page, length);
                }
                existingPage = page;
            }

            this.capacity = globalAllocator.insertMultiplePages(pageId, existingPage);

            if (existingPage != null) {
                // We had an oversized page and already inserted it
                // We return null to tell the caller that the data doesn't need to be inserted again
                return null;
            }
            return page;
        }
    }
}
