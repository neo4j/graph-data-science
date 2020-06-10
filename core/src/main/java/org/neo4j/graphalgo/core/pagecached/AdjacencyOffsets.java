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

import org.neo4j.graphalgo.core.loading.ImportSizing;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

public abstract class AdjacencyOffsets {

    abstract long get(long index);

    abstract long release();

    public static AdjacencyOffsets of(long[][] pages, int pageSize) {
        if (pages.length == 1) {
            return new SinglePageOffsets(pages[0]);
        }
        return new PagedOffsets(pages, pageSize);
    }

    public static AdjacencyOffsets of(PagedFile pagedFile) throws IOException {
        PageCursor pageCursor = pagedFile.io(
            0,
            PagedFile.PF_SHARED_READ_LOCK,
            PageCursorTracer.NULL
        );
        return new PageCachedOffsets(pageCursor);
    }

    static MemoryEstimation memoryEstimation(int pageSize, int numberOfPages) {
        if (numberOfPages == 1) {
            return SinglePageOffsets.memoryEstimation(pageSize);
        } else {
            return PagedOffsets.memoryEstimation(pageSize, numberOfPages);
        }
    }

    public static MemoryEstimation memoryEstimation(int concurrency, long nodeCount) {
        ImportSizing importSizing = ImportSizing.of(concurrency, nodeCount);
        return AdjacencyOffsets.memoryEstimation(
            importSizing.pageSize(),
            importSizing.numberOfPages());
    }

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup(
                "adjacency offsets",
                (dimensions, concurrency) -> memoryEstimation(concurrency, dimensions.nodeCount())
        );
    }

    public static AdjacencyOffsets of(long[] page) {
        return new SinglePageOffsets(page);
    }

    private static final class PagedOffsets extends AdjacencyOffsets {

        private final int pageShift;
        private final long pageMask;
        private long[][] pages;

        static MemoryEstimation memoryEstimation(int pageSize, int numberOfPages) {
            return MemoryEstimations.builder(AdjacencyOffsets.PagedOffsets.class)
                    .fixed("pages wrapper", sizeOfObjectArray(numberOfPages))
                    .fixed("page[]", sizeOfLongArray(pageSize) * numberOfPages)
                    .build();
        }

        private PagedOffsets(long[][] pages, int pageSize) {
            assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = pageSize - 1;
            this.pages = pages;
        }

        @Override
        long get(long index) {
            final int pageIndex = (int) (index >>> pageShift);
            final int indexInPage = (int) (index & pageMask);
            return pages[pageIndex][indexInPage];
        }

        @Override
        long release() {
            if (pages != null) {
                long memoryUsed = sizeOfObjectArray(pages.length);
                for (long[] page : pages) {
                    memoryUsed += sizeOfLongArray(page.length);
                }
                pages = null;
                return memoryUsed;
            }
            return 0L;
        }
    }

    private static final class SinglePageOffsets extends AdjacencyOffsets {

        private long[] page;

        static MemoryEstimation memoryEstimation(int pageSize) {
            return MemoryEstimations.builder(AdjacencyOffsets.SinglePageOffsets.class)
                    .fixed("page", sizeOfLongArray(pageSize))
                    .build();
        }

        private SinglePageOffsets(long[] page) {
            this.page = page;
        }

        @Override
        long get(long index) {
            return page[(int) index];
        }

        @Override
        long release() {
            if (page != null) {
                long memoryUsed = sizeOfLongArray(page.length);
                page = null;
                return memoryUsed;
            }
            return 0L;
        }
    }

    private static final class PageCachedOffsets extends AdjacencyOffsets {

        private PageCursor pageCursor;

        private PageCachedOffsets(PageCursor pageCursor) {
            this.pageCursor = pageCursor;
        }

        @Override
        long get(long index) {
            var pageSize = pageCursor.getCurrentPageSize();
            var longsPerPage = pageSize / Long.BYTES;
            var pageIndex = index / longsPerPage;
            var indexInPage = (int)((index % longsPerPage) * Long.BYTES);
            try {
                if (!pageCursor.next(pageIndex)) {
                    throw new ArrayIndexOutOfBoundsException("Array index out of range: " + index);
                }
                return pageCursor.getLong(indexInPage);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        long release() {
            pageCursor.close();
            return 0L;
        }
    }
}
