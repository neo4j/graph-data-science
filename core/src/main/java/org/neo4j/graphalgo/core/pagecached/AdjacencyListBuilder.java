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

import org.eclipse.collections.impl.factory.Sets;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

final class AdjacencyListBuilder {

    private static AtomicInteger GENERATION = new AtomicInteger(0);

//    private static final long PAGE_SIZE_IN_BYTES = sizeOfByteArray(PAGE_SIZE);
//    private static final AtomicReferenceFieldUpdater<AdjacencyListBuilder, byte[][]> PAGES_UPDATER =
//        AtomicReferenceFieldUpdater.newUpdater(AdjacencyListBuilder.class, byte[][].class, "pages");
//    private static final int NO_SKIP = -1;

    private final PageCache pageCache;
    private final AllocationTracker tracker;

    private final AtomicInteger allocatedPages;

    private final PagedFile pagedFile;

    static AdjacencyListBuilder newBuilder(PageCache pageCache, AllocationTracker tracker) {
        return new AdjacencyListBuilder(pageCache, tracker);
    }

    private AdjacencyListBuilder(PageCache pageCache, AllocationTracker tracker) {
        this.pageCache = pageCache;
        this.tracker = tracker;
        this.allocatedPages = new AtomicInteger(0);
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
    }

    Allocator newAllocator() {
        return new Allocator(this);
    }

    public AdjacencyList build() throws IOException {
        pagedFile.flushAndForce();
        return new AdjacencyList(pagedFile);
    }

    private long insertDefaultSizedPage(Allocator into) throws IOException {
        int pageIndex = allocatedPages.getAndIncrement();
        into.setNewPages(pageIndex);

//        grow(pageIndex + 1, NO_SKIP);
        return ((long) pagedFile.pageSize()) * pageIndex;
//        PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
//        into.setNewPages(pages, intoIndex);
//        return intoIndex;
    }

    private long insertDefaultSizedPages(Allocator into, int numPages) throws IOException {
        int pageIndex = allocatedPages.getAndAdd(numPages);
        into.setNewPages(pageIndex);

//        grow(pageIndex + 1, NO_SKIP);
        return ((long) pagedFile.pageSize()) * pageIndex;
//        PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
//        into.setNewPages(pages, intoIndex);
//        return intoIndex;
    }
//
//    private long insertOversizedPage(byte[] page, Allocator into) {
//        int pageIndex = allocatedPages.getAndIncrement();
//        grow(pageIndex + 1, pageIndex);
//
//        // We already increased `pages` for the oversize page in `grow()`.
//        // We need to insert the new page at the right position and
//        // remove the previously tracked memory. This has to happen
//        // within the grow lock to avoid the `pages` reference to be
//        // overwritten by another thread during `grow()`.
//        growLock.lock();
//        try {
//            tracker.add(sizeOfByteArray(page.length));
//            if (PAGES_UPDATER.get(this)[pageIndex] != null) {
//                tracker.remove(PAGE_SIZE_IN_BYTES);
//            }
//            PAGES_UPDATER.get(this)[pageIndex] = page;
//        } finally {
//            growLock.unlock();
//        }
//        into.insertPage(page);
//
//        return PageUtil.capacityFor(pageIndex, PAGE_SHIFT);
//    }
//
//    private void grow(int newNumPages, int skipPage) {
//        if (capacityLeft(newNumPages)) {
//            return;
//        }
//        growLock.lock();
//        try {
//            if (capacityLeft(newNumPages)) {
//                return;
//            }
//            setPages(newNumPages, skipPage);
//        } finally {
//            growLock.unlock();
//        }
//    }
//
//    private boolean capacityLeft(long newNumPages) {
//        return newNumPages <= PAGES_UPDATER.get(this).length;
//    }
//
//    /**
//     * Grows and re-assigns the {@code pages} representing the Adjacency List.
//     *
//     * This method is not thread-safe.
//     * Callers need to acquire the {@code growLock} before entering the method.
//     */
//    private void setPages(int newNumPages, int skipPage) {
//        byte[][] currentPages = PAGES_UPDATER.get(this);
//        tracker.add(sizeOfObjectArrayElements(newNumPages - currentPages.length));
//
//        byte[][] newPages = Arrays.copyOf(currentPages, newNumPages);
//
//        for (int i = currentPages.length; i < newNumPages; i++) {
//            // Create new page for default sized pages
//            if (i != skipPage) {
//                tracker.add(PAGE_SIZE_IN_BYTES);
//                newPages[i] = new byte[PAGE_SIZE];
//            }
//        }
//        PAGES_UPDATER.set(this, newPages);
//    }

    private static File file() {
        return new File("gds.adjacency." + GENERATION.getAndIncrement());
    }

    static final class Allocator {

        private final AdjacencyListBuilder builder;

        private final PageCursor pageCursor;

        private long top;

//        private byte[][] pages;
//        private int prevOffset;
//        private int toPageIndex;
//        private int currentPageIndex;

//        public byte[] page;
//        public int offset;

        private Allocator(AdjacencyListBuilder builder) {
            this.builder = builder;
            try {
                this.pageCursor = Neo4jProxy.pageFileIO(
                    builder.pagedFile,
                    0,
                    PagedFile.PF_SHARED_WRITE_LOCK,
                    PageCursorTracer.NULL
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
//            prevOffset = -1;
        }

        void prepare() throws IOException {
            top = builder.insertDefaultSizedPage(this);
            if (top == 0L) {
                ++top;
                pageCursor.setOffset(pageCursor.getOffset() + 1);
            }
        }

        long allocate(int size) throws IOException {
            return localAllocate(size, top);
        }

        void putInt(int value) {
            pageCursor.putInt(value);
        }

        void putLong(long value) {
            pageCursor.putLong(value);
        }

        void close() {
            pageCursor.close();
        }

        long insert(byte[] bytes, int arrayOffset, int length) throws IOException {
            int maxOffset = pageCursor.getCurrentPageSize() - length;

            // data fits in current page
            if (maxOffset >= pageCursor.getOffset()) {
                pageCursor.putBytes(bytes, arrayOffset, length);
                long address = top;
                top += length;
                return address;
            }

            long address = prefetchAllocate(length);
            while (length > 0) {
                int availableSpace = Math.min(length, pageCursor.getCurrentPageSize() - pageCursor.getOffset());
                pageCursor.putBytes(bytes, arrayOffset, availableSpace);
                length -= availableSpace;
                arrayOffset += availableSpace;

                if (length > 0) {
                    pageCursor.next();
                }
            }

            return address;
        }


        private long localAllocate(int size, long address) throws IOException {
            int maxOffset = pageCursor.getCurrentPageSize() - size;
            if (maxOffset >= pageCursor.getOffset()) {
                top += size;
                return address;
            }
            return prefetchAllocate(size);
        }

//        private long majorAllocate(int size, int maxOffset, long address) {
//            if (maxOffset < 0) {
//                return oversizingAllocate(size);
//            }
//            if (reset() && maxOffset >= offset) {
//                top += size;
//                return address;
//            }
//            int waste = PAGE_SIZE - offset;
//            address = top += waste;
//            if (next()) {
//                // TODO: store and reuse fragments
//                // branch: huge-alloc-fragmentation-recycle
//                top += size;
//                return address;
//            }
//            return prefetchAllocate(size);
//        }
//
//        /**
//         * We are faking a valid page by over-allocating a single page to be large enough to hold all data
//         * Since we are storing all degrees into a single page and thus never have to switch pages
//         * and keep the offsets as if this page would be of the correct size, we might just get by.
//         */
//        private long oversizingAllocate(int size) {
//            byte[] largePage = new byte[size];
//            return builder.insertOversizedPage(largePage, this);
//        }

        private long prefetchAllocate(int size) throws IOException {
            long address = top = builder.insertDefaultSizedPages(this, (int) ceilDiv(size, builder.pagedFile.pageSize()));
            top += size;
            return address;
        }

//        private boolean reset() {
//            if (prevOffset != -1) {
//                page = pages[currentPageIndex];
//                offset = prevOffset;
//                prevOffset = -1;
//                return true;
//            }
//            return false;
//        }
//
//        private boolean next() {
//            if (++currentPageIndex <= toPageIndex) {
//                page = pages[currentPageIndex];
//                offset = 0;
//                return true;
//            }
//            page = null;
//            return false;
//        }

        private void setNewPages(long pageId) throws IOException {
            boolean next = pageCursor.next(pageId);
            if (!next) {
                throw new IllegalStateException("This should never happen");
            }
//            assert PageUtil.indexInPage(pageId, PAGE_MASK) == 0;
//            currentPageIndex = PageUtil.pageIndex(pageId, PAGE_SHIFT);
//            toPageIndex = currentPageIndex;
//            page = pages[currentPageIndex];
//            offset = 0;
        }

//        private void insertPage(byte[] page) {
//            if (prevOffset == -1) {
//                prevOffset = offset;
//            }
//            this.page = page;
//            offset = 0;
//        }
    }
}
