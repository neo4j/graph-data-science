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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.core.loading.IdMapAllocator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HugeLongArrayBuilder {

    private long[][] pages;
    private final Lock lock;

    private static final VarHandle PAGES;

    static {
        try {
            PAGES = MethodHandles.lookup().findVarHandle(HugeLongArrayBuilder.class, "pages", long[][].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static HugeLongArrayBuilder newBuilder() {
        return new HugeLongArrayBuilder();
    }

    HugeLongArrayBuilder() {
        this.pages = new long[0][0];
        this.lock = new ReentrantLock();
    }

    public HugeLongArray build(long size) {
        // make sure that we return the latest, correct version of pages
        VarHandle.fullFence();
        var pages = (long[][]) PAGES.getVolatile(this);
        return HugeLongArray.of(pages, size);
    }

    public void allocate(long start, int batchLength, Allocator allocator) {
        var endPage = HugeArrays.pageIndex(start + batchLength - 1);
        var pages = (long[][]) PAGES.getAcquire(this);
        if (endPage >= pages.length) {
            lock.lock();
            try {
                pages = (long[][]) PAGES.getVolatile(this);
                if (endPage >= pages.length) {
                    var newPages = Arrays.copyOf(pages, endPage + 1);
                    for (int i = newPages.length - 1; i >= 0; i--) {
                        if (newPages[i] != null) {
                            break;
                        }
                        newPages[i] = new long[HugeArrays.PAGE_SIZE];
                    }

                    PAGES.setRelease(this, newPages);
                    pages = newPages;
                }
            }
            finally {
                lock.unlock();
            }
        }

        // Why don't we need to declare pages as volatile?
        // acquire is ok since we only change the outer layer 'pages', not the actual inner
        // pages where we write into. Take the following example, using a page size of 3:
        // page size = 3
        // t1 -> [ 0..1, 1..2, 5..6 ]  // wants to write at those positions
        // t2 -> [ 2..5 ]
        // t1 -> 0..1 -> no grow  @1[ @23[000] ] -> @1[ @23[x00] ]
        // t2 -> 2..5 ->    grow  @1[ @23[x00] ] -> @2[ @23[x0y], @45[yy0] ]
        // t1 -> 1..2 -> no grow  @1[ @23[xzy] ]   (plain read on "old" @1 pages) <- this is still writing to the correct page
        //                        @1[ @23[xz0] ]   (the write to z could also not be observed, which is fine since
        //                                          we write in a different location and Java guarantees us that those
        //                                          will eventually all be visible)
        // t1 -> 5..6 ->    grow  -> lock -> (memory barrier from lock refreshes pages to @2) -> unlock -> @2[ @23[xzy], @45[yya] ]
        allocator.reset(start, start + batchLength, pages);
    }

    public static final class Allocator implements IdMapAllocator, AutoCloseable {
        private long[] buffer;
        private int allocationSize;
        private int offset;
        private int length;

        private final HugeCursor.PagedCursor<long[]> cursor;

        public Allocator() {
            this.cursor = new HugeCursor.PagedCursor<>(new long[0][]);
        }

        private void reset(long start, long end, long[][] pages) {
            this.cursor.setPages(pages);
            this.cursor.setRange(start, end);
            this.buffer = null;
            this.allocationSize = (int) (end - start);
            this.offset = 0;
            this.length = 0;
        }

        public boolean nextBuffer() {
            if (!cursor.next()) {
                return false;
            }
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }

        @Override
        public int allocatedSize() {
            return this.allocationSize;
        }

        @Override
        public void insert(long[] nodeIds) {
            int batchOffset = 0;
            while (nextBuffer()) {
                System.arraycopy(nodeIds, batchOffset, this.buffer, this.offset, this.length);
                batchOffset += this.length;
            }
        }

        @Override
        public void close() {
            this.cursor.close();
        }
    }

}
