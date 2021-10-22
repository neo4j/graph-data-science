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
package org.neo4j.gds.collections;

import org.neo4j.gds.mem.MemoryUsage;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;

public final class HugeSparseLongArrayFoo implements HugeSparseLongArray {
    private static final int PAGE_SHIFT = 12;

    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;

    private static final int PAGE_MASK = PAGE_SIZE - 1;

    private static final long PAGE_SIZE_IN_BYTES = MemoryUsage.sizeOfLongArray(PAGE_SIZE);

    private final long capacity;

    private final long[][] pages;

    private final long defaultValue;

    private HugeSparseLongArrayFoo(long capacity, long[][] pages, long defaultValue) {
        this.capacity = capacity;
        this.pages = pages;
        this.defaultValue = defaultValue;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public long get(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        if (pageIndex < pages.length) {
            long[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage];
            }
        }
        return defaultValue;
    }

    @Override
    public boolean contains(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        if (pageIndex < pages.length) {
            long[] page = pages[pageIndex];
            if (page != null) {
                int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
                return page[indexInPage] != defaultValue;
            }
        }
        return false;
    }

    public DrainingIterator drainingIterator() {
        return new DrainingIterator(pages);
    }

    public DrainingBatch drainingBatch() {
        return new DrainingBatch(defaultValue);
    }

    public static final class GrowingBuilder implements Builder {
        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        private final ReentrantLock newPageLock;

        private final long defaultValue;

        private AtomicReferenceArray<long[]> pages;

        private final LongConsumer trackAllocation;

        GrowingBuilder(long defaultValue, long initialCapacity, LongConsumer trackAllocation) {
            int pageCount = PageUtil.pageIndex(initialCapacity, PAGE_SHIFT);
            this.pages = new AtomicReferenceArray<long[]>(pageCount);
            this.defaultValue = defaultValue;
            this.newPageLock = new ReentrantLock(true);
            this.trackAllocation = trackAllocation;
        }

        @Override
        public void set(long index, long value) {
            int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
            int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
            ARRAY_HANDLE.setVolatile(getPage(pageIndex), indexInPage, value);
        }

        @Override
        public HugeSparseLongArray build() {
            int numPages = pages.length();
            long capacity = ((long) numPages) << PAGE_SHIFT;
            long[][] newPages = new long[numPages][];
            Arrays.setAll(newPages, pages::get);
            return new HugeSparseLongArrayFoo(capacity, newPages, defaultValue);
        }

        @Override
        public boolean setIfAbsent(long index, long value) {
            int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
            int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
            long storedValue = (long) ARRAY_HANDLE.compareAndExchange(getPage(pageIndex), indexInPage, defaultValue, value);
            return storedValue == defaultValue;
        }

        @Override
        public void addTo(long index, long value) {
            int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
            int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
            long[] page = getPage(pageIndex);
            long expectedCurrentValue = (long) ARRAY_HANDLE.getVolatile(page, indexInPage);
            while (true) {
                long newValueToStore = (long) (expectedCurrentValue + value);
                long actualCurrentValue = (long) ARRAY_HANDLE.compareAndExchange(page, indexInPage, expectedCurrentValue, newValueToStore);
                if (actualCurrentValue == expectedCurrentValue) {
                    return;
                }
                expectedCurrentValue = actualCurrentValue;
            }
        }

        private void grow(int newSize) {
            newPageLock.lock();
            try {
                if (newSize <= pages.length()) {
                    return;
                }
                AtomicReferenceArray<long[]> newPages = new AtomicReferenceArray<long[]>(org.neo4j.gds.mem.HugeArrays.oversizeInt(newSize, MemoryUsage.BYTES_OBJECT_REF));
                for (int pageIndex = 0; pageIndex < pages.length(); pageIndex++) {
                    long[] page = pages.get(pageIndex);
                    if (page != null) {
                        newPages.set(pageIndex, page);
                    }
                }
                pages = newPages;
            }
            finally {
                newPageLock.unlock();
            }
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

        private long[] allocateNewPage(int pageIndex) {
            newPageLock.lock();
            try {
                long[] page = pages.get(pageIndex);
                if (page != null) {
                    return page;
                }
                trackAllocation.accept(PAGE_SIZE_IN_BYTES);
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

    public static final class DrainingIterator {

        private final long[][] pages;
        private final AtomicInteger globalPageId;

        private DrainingIterator(long[][] pages) {
            this.pages = pages;
            globalPageId = new AtomicInteger(0);
        }

        public boolean next(DrainingBatch reuseBatch) {
            int nextPageId = 0;
            long[] nextPage = null;

            while (nextPage == null) {
                nextPageId = globalPageId.getAndIncrement();

                if(nextPageId >= pages.length) {
                    return false;
                }

                nextPage = pages[nextPageId];
            }

            // clear the reference to the page
            pages[nextPageId] = null;

            reuseBatch.reset(nextPage, (long) nextPageId * PAGE_SIZE);

            return true;
        }
    }

    public static final class DrainingBatch  {
        private final long defaultValue;

        private long[] page;
        private long offset;

        private DrainingBatch(long defaultValue) {
            this.defaultValue = defaultValue;
        }

        public void reset(long[] page, long offset) {
            this.page = page;
            this.offset = offset;
        }

        public void consume(LongLongConsumer consumer) {
            for (int pageIndex = 0; pageIndex < page.length; pageIndex++) {
                var value = page[pageIndex];
                if (value != defaultValue) {
                    consumer.consume(offset + pageIndex, value);
                }
            }
        }

        public interface LongLongConsumer {
            void consume(long index, long value);
        }
    }
}
