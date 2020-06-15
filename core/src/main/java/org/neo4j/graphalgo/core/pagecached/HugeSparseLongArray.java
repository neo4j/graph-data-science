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
package org.neo4j.graphalgo.core.pagecached;

import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

public final class HugeSparseLongArray implements  AutoCloseable {

    private static final AtomicInteger GENERATION = new AtomicInteger(0);

    private static final long NOT_FOUND = -1L;

    private static final int PAGE_SHIFT = 12;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long PAGE_SIZE_IN_BYTES = MemoryUsage.sizeOfLongArray(PAGE_SIZE);

    private final PagedFile pagedFile;
    private final long capacity;
    private final PageCursor pageCursor;

    private HugeSparseLongArray(PagedFile pagedFile, long capacity) {
        this.pagedFile = pagedFile;
        this.capacity = capacity;
        try {
            pageCursor = Neo4jProxy.pageFileIO(
                pagedFile,
                0,
                PagedFile.PF_SHARED_READ_LOCK,
                PageCursorTracer.NULL
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

//    /**
//     * @param maxId highest id that we need to represent
//     *             (equals size in {@link Builder#create(long, AllocationTracker)})
//     * @param maxEntries number of identifiers we need to store
//     */
//    public static MemoryRange memoryEstimation(long maxId, long maxEntries) {
//        assert(maxEntries <= maxId);
//        int numPagesForSize = PageUtil.numPagesFor(maxId, PAGE_SHIFT, PAGE_MASK);
//        int numPagesForMaxEntriesBestCase = PageUtil.numPagesFor(maxEntries, PAGE_SHIFT, PAGE_MASK);
//
//        // Worst-case distribution assumes at least one entry per page
//        final long maxEntriesForWorstCase = Math.min(maxId, maxEntries * PAGE_SIZE);
//        int numPagesForMaxEntriesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, PAGE_SHIFT, PAGE_MASK);
//
//        long classSize = MemoryUsage.sizeOfInstance(HugeSparseLongArray.class);
//        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);
//        long minRequirements = numPagesForMaxEntriesBestCase * PAGE_SIZE_IN_BYTES;
//        long maxRequirements = numPagesForMaxEntriesWorstCase * PAGE_SIZE_IN_BYTES;
//        return MemoryRange.of(classSize + pagesSize).add(MemoryRange.of(minRequirements, maxRequirements));
//    }

    public long getCapacity() {
        return capacity;
    }

    public long get(long index) throws IOException {
        int pageIndex = (int)(index / PageCache.PAGE_SIZE);
        int indexInPage = (int)(index % PageCache.PAGE_SIZE);
        pageCursor.next(pageIndex);
        long aLong = pageCursor.getLong(indexInPage * Long.BYTES);
        return aLong;
    }

    public boolean contains(long index) throws IOException {
        int pageIndex = (int)(index / PageCache.PAGE_SIZE);
        int indexInPage = (int)(index % PageCache.PAGE_SIZE);
        pageCursor.next(pageIndex);
        return pageCursor.getLong(indexInPage) != NOT_FOUND;
    }

    @Override
    public void close() throws Exception {
        pageCursor.close();
        pagedFile.flushAndForce();
    }

    static File file() {
        return new File("gds.neo_to_id_mapping." + GENERATION.getAndIncrement());
    }

    public static final class Builder {

        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

        private static final byte[] EMPTY_PAGE;
        static {
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[PageCache.PAGE_SIZE]);
            for (int i = 0; i < PageCache.PAGE_SIZE / Long.BYTES; i++) {
                byteBuffer.putLong(NOT_FOUND);
            }
            EMPTY_PAGE = byteBuffer.array();
        }

        private final PagedFile pagedFile;
        private final Set<Integer> initializedPages;

        private final long capacity;
        private final long defaultValue;

        private final AtomicReferenceArray<long[]> pages;
        private final AllocationTracker tracker;
        private final ReentrantLock newPageLock;
        private final PageCursor pageCursor;

        public static Builder create(PageCache pageCache, long size, AllocationTracker tracker) {
            return create(pageCache, size, NOT_FOUND, tracker);
        }

        public static Builder create(
            PageCache pageCache,
            long size,
            long defaultValue,
            AllocationTracker tracker
        )  {
            PagedFile pagedFile;
            try {
                pagedFile = Neo4jProxy.pageCacheMap(
                    pageCache,
                    file(),
                    PageCache.PAGE_SIZE,
                    StandardOpenOption.CREATE
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, PAGE_MASK);
            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
            AtomicReferenceArray<long[]> pages = new AtomicReferenceArray<>(numPages);
            tracker.add(MemoryUsage.sizeOfObjectArray(numPages));
            return new Builder(pagedFile, capacity, pages, defaultValue, tracker);
        }

        private Builder(
            PagedFile pagedFile,
            long capacity,
            AtomicReferenceArray<long[]> pages,
            long defaultValue,
            AllocationTracker tracker
        ) {
            this.pagedFile = pagedFile;
            try {
                pageCursor = Neo4jProxy.pageFileIO(
                    pagedFile,
                    0,
                    PagedFile.PF_SHARED_WRITE_LOCK,
                    PageCursorTracer.NULL
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            this.initializedPages = Collections.newSetFromMap(new ConcurrentHashMap<>());
            this.capacity = capacity;
            this.pages = pages;
            this.tracker = tracker;
            this.defaultValue = defaultValue;
            this.newPageLock = new ReentrantLock(true);
        }

        public void set(long index, long value) throws IOException {
            assert index < capacity;
            final int pageIndex = (int)(index / PageCache.PAGE_SIZE);
            if (initializedPages.add(pageIndex)) {
                initializePage(pageIndex);
            } else {
                pageCursor.next(pageIndex);
            }
            final int indexInPage = (int)(index % PageCache.PAGE_SIZE);
            pageCursor.putLong(indexInPage * Long.BYTES, value);
        }

        synchronized void initializePage(int pageIndex) throws IOException {
            pageCursor.next(pageIndex);
            pageCursor.putBytes(EMPTY_PAGE);
            initializedPages.add(pageIndex);
        }

//        /**
//         * @return true iff the value was absent and was added, false if there already was a value at this position.
//         */
//        public boolean setIfAbsent(long index, long value) {
//            assert index < capacity;
//            final int pageIndex = pageIndex(index);
//            final int indexInPage = indexInPage(index);
//            long[] page = pages.get(pageIndex);
//            if (page == null) {
//                page = allocateNewPage(pageIndex);
//            }
//
//            long storedValue = (long) ARRAY_HANDLE.compareAndExchange(
//                page,
//                indexInPage,
//                NOT_FOUND,
//                value
//            );
//            return storedValue == NOT_FOUND;
//        }

        public HugeSparseLongArray build() throws IOException {
//            int numPages = this.pages.length();
//            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
//            long[][] pages = new long[numPages][];
//            Arrays.setAll(pages, this.pages::get);
            pageCursor.close();
            pagedFile.flushAndForce();
            return new HugeSparseLongArray(pagedFile, capacity);
        }
    }

//    public static final class GrowingBuilder {
//        private final AllocationTracker tracker;
//        private final ReentrantLock newPageLock;
//        private final long defaultValue;
//
//        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
//
//        private AtomicReferenceArray<long[]> pages;
//
//        public static GrowingBuilder create(AllocationTracker tracker) {
//            return create(NOT_FOUND, tracker);
//        }
//
//        public static GrowingBuilder create(long defaultValue, AllocationTracker tracker) {
//            AtomicReferenceArray<long[]> pages = new AtomicReferenceArray<>(0);
//            return new GrowingBuilder(pages, defaultValue, tracker);
//        }
//
//        private GrowingBuilder(AtomicReferenceArray<long[]> pages, long defaultValue, AllocationTracker tracker) {
//            this.pages = pages;
//            this.tracker = tracker;
//            this.defaultValue = defaultValue;
//            this.newPageLock = new ReentrantLock(true);
//        }
//
//        public void set(long index, long value) {
//            int pageIndex = pageIndex(index);
//            int indexInPage = indexInPage(index);
//            ARRAY_HANDLE.setVolatile(getPage(pageIndex), indexInPage, value);
//        }
//
//        public void addTo(long index, long value) {
//            int pageIndex = pageIndex(index);
//            int indexInPage = indexInPage(index);
//            long[] page = getPage(pageIndex);
//
//            long expectedCurrentValue = (long) ARRAY_HANDLE.getVolatile(page, indexInPage);
//
//            while (true) {
//                var newValueToStore = expectedCurrentValue + value;
//                long actualCurrentValue = (long) ARRAY_HANDLE.compareAndExchange(
//                    page,
//                    indexInPage,
//                    expectedCurrentValue,
//                    newValueToStore
//                );
//                if (actualCurrentValue == expectedCurrentValue) {
//                    return ;
//                }
//                expectedCurrentValue = actualCurrentValue;
//            }
//        }
//
//        public HugeSparseLongArray build() {
//            int numPages = this.pages.length();
//            long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
//            long[][] pages = new long[numPages][];
//            Arrays.setAll(pages, this.pages::get);
//            return new HugeSparseLongArray(capacity, pages);
//        }
//
//        private long[] getPage(int pageIndex) {
//            if (pageIndex >= pages.length()) {
//                grow(pageIndex + 1);
//            }
//
//            long[] page = pages.get(pageIndex);
//            if (page == null) {
//                page = allocateNewPage(pageIndex);
//            }
//            return page;
//        }
//
//        private void grow(int newSize) {
//            newPageLock.lock();
//            try {
//                if (newSize <= pages.length()) {
//                    return;
//                }
//
//                AtomicReferenceArray<long[]> newPages = new AtomicReferenceArray<>(oversize(newSize, MemoryUsage.BYTES_OBJECT_REF));
//                for (int pageIndex = 0; pageIndex < pages.length(); pageIndex++) {
//                    long[] page = pages.get(pageIndex);
//                    if (page != null) {
//                        newPages.set(pageIndex, page);
//                    }
//                }
//                this.pages = newPages;
//            } finally {
//                newPageLock.unlock();
//            }
//        }
//
//        private long[] allocateNewPage(int pageIndex) {
//            newPageLock.lock();
//            try {
//                long[] page = pages.get(pageIndex);
//                if (page != null) {
//                    return page;
//                }
//                tracker.add(PAGE_SIZE_IN_BYTES);
//                page = new long[PAGE_SIZE];
//                if (defaultValue != 0L) {
//                    Arrays.fill(page, defaultValue);
//                }
//                pages.set(pageIndex, page);
//                return page;
//            } finally {
//                newPageLock.unlock();
//            }
//        }
//    }
}
