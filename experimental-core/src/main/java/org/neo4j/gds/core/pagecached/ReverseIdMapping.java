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
package org.neo4j.gds.core.pagecached;

import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public final class ReverseIdMapping implements AutoCloseable {

    private static final AtomicInteger GENERATION = new AtomicInteger(0);

    private static final long NOT_FOUND = -1L;

    private final PagedFile pagedFile;
    private final PageCursor pageCursor;

    private ReverseIdMapping(PagedFile pagedFile) {
        this.pagedFile = pagedFile;
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

    public long get(long index) throws IOException {
        long longBaseIndex = index * Long.BYTES;
        int pageIndex = (int)(longBaseIndex / PageCache.PAGE_SIZE);
        int indexInPage = (int)(longBaseIndex % PageCache.PAGE_SIZE);
        pageCursor.next(pageIndex);
        return pageCursor.getLong(indexInPage);
    }

    public boolean contains(long index) throws IOException {
        long longBaseIndex = index * Long.BYTES;
        int pageIndex = (int)(longBaseIndex / PageCache.PAGE_SIZE);
        int indexInPage = (int)(longBaseIndex % PageCache.PAGE_SIZE);
        pageCursor.next(pageIndex);
        return pageCursor.getLong(indexInPage) != NOT_FOUND;
    }

    @Override
    public void close() throws IOException {
        pageCursor.close();
        pagedFile.flushAndForce();
    }

    private static File file() {
        return new File("gds.neo_to_id_mapping." + GENERATION.getAndIncrement());
    }

    public static final class Builder {

        private final byte[] emptyPage;

        private final PagedFile pagedFile;
        private final Set<Integer> initializedPages;

        private final long capacity;

        private final PageCursor pageCursor;

        public static Builder create(PageCache pageCache, long size) {
            return create(pageCache, size, NOT_FOUND);
        }

        public static Builder create(
            PageCache pageCache,
            long size,
            long defaultValue
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
            long numPages = ceilDiv(size * Long.BYTES, PageCache.PAGE_SIZE);
            long capacity = numPages * PageCache.PAGE_SIZE;
            return new Builder(pagedFile, capacity, defaultValue);
        }

        private Builder(
            PagedFile pagedFile,
            long capacity,
            long defaultValue
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
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[PageCache.PAGE_SIZE]);
            for (int i = 0; i < PageCache.PAGE_SIZE / Long.BYTES; i++) {
                byteBuffer.putLong(defaultValue);
            }
            emptyPage = byteBuffer.array();
            this.initializedPages = Collections.newSetFromMap(new ConcurrentHashMap<>());
            this.capacity = capacity;
        }

        public void set(long index, long value) throws IOException {
            long longBasedIndex = index * Long.BYTES;
            assert longBasedIndex < capacity;
            final int pageIndex = (int)(longBasedIndex / PageCache.PAGE_SIZE);
            if (initializedPages.add(pageIndex)) {
                initializePage(pageIndex);
            } else {
                pageCursor.next(pageIndex);
            }
            final int indexInPage = (int)(longBasedIndex % PageCache.PAGE_SIZE);
            pageCursor.putLong(indexInPage, value);
        }

        synchronized void initializePage(int pageIndex) throws IOException {
            pageCursor.next(pageIndex);
            pageCursor.putBytes(emptyPage);
        }

        public ReverseIdMapping build() throws IOException {
            initializeUntouchedPages();
            pageCursor.close();
            pagedFile.flushAndForce();
            return new ReverseIdMapping(pagedFile);
        }

        private void initializeUntouchedPages() throws IOException {
            long numPages = (capacity * Long.BYTES) / PageCache.PAGE_SIZE;
            for (int i = 0; i < numPages; i++) {
                if (!initializedPages.contains(i)) {
                    initializePage(i);
                }
            }
        }
    }
}
