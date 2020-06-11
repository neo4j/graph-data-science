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
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AdjacencyOffsets {

    private static AtomicInteger GENERATION = new AtomicInteger(0);

    abstract long get(long index) throws IOException;

    abstract long release();

    public static AdjacencyOffsets of(PageCache pageCache, long[][] pages) throws IOException {
        PagedFile pagedFile = pageCache.map(
            file(),
            PageCache.PAGE_SIZE,
            Sets.immutable.of(StandardOpenOption.CREATE)
        );
        PageCursor pageCursor = pagedFile.io(
            0,
            PagedFile.PF_SHARED_WRITE_LOCK,
            PageCursorTracer.NULL
        );
        return new PageCachedOffsets(pagedFile, pageCursor, pages);
    }

    private static File file() {
        return new File("gds.offsets." + GENERATION.getAndIncrement());
    }

    private static final class PageCachedOffsets extends AdjacencyOffsets {

        private final PagedFile pagedFile;
        private final PageCursor pageCursor;
        private final long[][] pages;

        private PageCachedOffsets(PagedFile pagedFile, PageCursor pageCursor, long[][] pages) throws IOException {
            this.pagedFile = pagedFile;
            this.pageCursor = pageCursor;
            this.pages = pages;

            writeOffsetsToPageCache();
        }

        @Override
        long get(long index) throws IOException {
            var pageSize = PageCache.PAGE_SIZE;
            var longsPerPage = pageSize / Long.BYTES;
            var pageIndex = index / longsPerPage;
            var indexInPage = (int)((index % longsPerPage) * Long.BYTES);
            try {
                if (!pageCursor.next(pageIndex)) {
                    throw new ArrayIndexOutOfBoundsException("Array index out of range: " + index);
                }
                long aLong = pageCursor.getLong(indexInPage);
                return aLong;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        long release() {
            pageCursor.close();
            pagedFile.close();
            return 0L;
        }

        private void writeOffsetsToPageCache() throws IOException {
            pageCursor.next();
            for (long[] values : pages) {
                for (long value : values) {
                    if (pageCursor.getOffset() >= pageCursor.getCurrentPageSize()) {
                        pageCursor.next();
                    }
                    pageCursor.putLong(value);
                }
            }
            pagedFile.flushAndForce();
        }
    }
}
