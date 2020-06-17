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
import org.neo4j.graphalgo.utils.CloseableThreadLocal;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class HugeLongArrayBuilder implements AutoCloseable {

    private static final AtomicInteger GENERATION = new AtomicInteger(0);

    private final long nodeCount;
    private final long lengthInBytes;
    private final AtomicLong allocationIndex;
    private final CloseableThreadLocal<BulkAdder> adders;
    private final PagedFile pagedFile;

    public static HugeLongArrayBuilder of(long length, PageCache pageCache) {
        return new HugeLongArrayBuilder(pageCache, length);
    }

    private HugeLongArrayBuilder(PageCache pageCache, final long nodeCount) {
        this.nodeCount = nodeCount;
        this.lengthInBytes = Long.BYTES * nodeCount;
        this.allocationIndex = new AtomicLong();
        this.adders = CloseableThreadLocal.withInitial(this::newBulkAdder);

        try {
            this.pagedFile = Neo4jProxy.pageCacheMap(
                pageCache,
                file(),
                PageCache.PAGE_SIZE,
                StandardOpenOption.CREATE
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    BulkAdder allocate(long nodeCount) throws IOException {
        long requiredBytes = Long.BYTES * nodeCount;
        long startIndex = allocationIndex.getAndAccumulate(requiredBytes, this::upperAllocation);
        if (startIndex == this.nodeCount) {
            return null;
        }

        BulkAdder adder = adders.get();

        long endIndex = upperAllocation(startIndex, requiredBytes);
        long lastPage = (endIndex + 1) / PageCache.PAGE_SIZE;
        for (long pageId = startIndex / PageCache.PAGE_SIZE; pageId < lastPage; pageId++) {
            adder.touch(pageId);
        }

        adder.reset(startIndex, endIndex);
        return adder;
    }

    @Override
    public void close() throws IOException {
        adders.close();
        pagedFile.flushAndForce();
    }

    private long upperAllocation(long lower, long bytes) {
        return Math.min(lengthInBytes, lower + bytes);
    }

    long nodeCount() {
        return nodeCount;
    }

    PagedFile build() throws IOException {
        close();
        return pagedFile;
    }

    private BulkAdder newBulkAdder() {
        try {
            var pageCursor = Neo4jProxy.pageFileIO(pagedFile, 0, PagedFile.PF_SHARED_WRITE_LOCK, PageCursorTracer.NULL);
            return new BulkAdder(pageCursor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static File file() {
        return new File("gds.id_to_neo_mapping." + GENERATION.getAndIncrement());
    }

    static final class BulkAdder implements AutoCloseable {

        private final PageCursor pageCursor;
        private long allocated;

        BulkAdder(PageCursor pageCursor) {
            this.pageCursor = pageCursor;
        }

        void insert(long[] nodeIds, int arrayOffset, int arrayLength) throws IOException {
            long length = (long) Long.BYTES * arrayLength;
            if (length != allocated) {
                throw new IllegalArgumentException("Can only add " + allocated / Long.BYTES + " nodes, but " + arrayLength + " were given");
            }
            for (int i = 0; i < arrayLength; i++) {
                if (pageCursor.getOffset() >= pageCursor.getCurrentPageSize()) {
                    pageCursor.next();
                }
                int index = arrayOffset + i;
                pageCursor.putLong(nodeIds[index]);
            }
        }

        void touch(long pageId) throws IOException {
            pageCursor.next(pageId);
        }

        void reset(long startIndex, long endIndex) throws IOException {
            var startPage = startIndex / PageCache.PAGE_SIZE;
            pageCursor.next(startPage);
            pageCursor.setOffset((int) (startIndex % PageCache.PAGE_SIZE));
            this.allocated = endIndex - startIndex;
        }

        @Override
        public void close() {
            pageCursor.close();
        }
    }
}
