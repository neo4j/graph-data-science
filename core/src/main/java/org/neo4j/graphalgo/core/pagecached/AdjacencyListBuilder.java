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

    private static final AtomicInteger GENERATION = new AtomicInteger(0);

    private final AtomicInteger allocatedPages;
    private final PagedFile pagedFile;

    static AdjacencyListBuilder newBuilder(PageCache pageCache) {
        return new AdjacencyListBuilder(pageCache);
    }

    private AdjacencyListBuilder(PageCache pageCache) {
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
        return ((long) pagedFile.pageSize()) * pageIndex;
    }

    private long insertDefaultSizedPages(Allocator into, int numPages) throws IOException {
        int pageIndex = allocatedPages.getAndAdd(numPages);
        into.setNewPages(pageIndex);
        return ((long) pagedFile.pageSize()) * pageIndex;
    }

    private static File file() {
        return new File("gds.adjacency." + GENERATION.getAndIncrement());
    }

    static final class Allocator {

        private final AdjacencyListBuilder builder;

        private final PageCursor pageCursor;

        private long top;

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

        private long prefetchAllocate(int size) throws IOException {
            long address = top = builder.insertDefaultSizedPages(this, (int) ceilDiv(size, builder.pagedFile.pageSize()));
            top += size;
            return address;
        }

        private void setNewPages(long pageId) throws IOException {
            boolean next = pageCursor.next(pageId);
            if (!next) {
                throw new IllegalStateException("This should never happen");
            }
        }
    }
}
