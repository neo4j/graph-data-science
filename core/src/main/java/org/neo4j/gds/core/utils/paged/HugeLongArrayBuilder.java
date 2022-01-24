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

import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.loading.IdMapAllocator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.Arrays;

public class HugeLongArrayBuilder {

    private final AllocationTracker allocationTracker;

    private long[][] pages;

    public static HugeLongArrayBuilder newBuilder(AllocationTracker allocationTracker) {
        return new HugeLongArrayBuilder(allocationTracker);
    }

    HugeLongArrayBuilder(AllocationTracker allocationTracker) {
        this.allocationTracker = allocationTracker;
        this.pages = new long[0][0];
    }

    public HugeLongArray build(long size) {
        return HugeLongArray.of(pages, size);
    }

    public void allocate(long start, int batchLength, Allocator allocator) {
        var endPage = HugeArrays.pageIndex(start + batchLength - 1);
        if (endPage >= this.pages.length) {
            synchronized (this) {
                if (endPage >= this.pages.length) {
                    this.pages = Arrays.copyOf(this.pages, endPage + 1);
                    for (int i = this.pages.length - 1; i >= 0; i--) {
                        if (this.pages[i] != null) {
                            break;
                        }
                        this.pages[i] = new long[HugeArrays.PAGE_SIZE];
                    }
                }
            }
        }
        var cursor = new HugeCursor.PagedCursor<>(PageUtil.capacityFor(this.pages.length, HugeArrays.PAGE_SHIFT), this.pages);
        cursor.setRange(start, start + batchLength);
        allocator.reset(start, start + batchLength, cursor);
    }

    public static final class Allocator implements IdMapAllocator {
        private long[] buffer;
        private int allocationSize;
        private long start;
        private int offset;
        private int length;
        private HugeCursor<long[]> cursor;

        private void reset(long start, long end, HugeCursor<long[]> cursor) {
            this.cursor = cursor;
            this.buffer = null;
            this.allocationSize = (int) (end - start);
            this.start = start;
            this.offset = 0;
            this.length = 0;
        }

        public boolean nextBuffer() {
            if (!cursor.next()) {
                return false;
            }
            start += length;
            buffer = cursor.array;
            offset = cursor.offset;
            length = cursor.limit - cursor.offset;
            return true;
        }

        @Override
        public long startId() {
            return start;
        }

        @Override
        public int allocatedSize() {
            return this.allocationSize;
        }

        @Override
        public void insert(long[] nodeIds, int length) {
            int batchOffset = 0;
            while (nextBuffer()) {
                System.arraycopy(nodeIds, batchOffset, this.buffer, this.offset, this.length);
                batchOffset += this.length;
            }
        }
    }

}
