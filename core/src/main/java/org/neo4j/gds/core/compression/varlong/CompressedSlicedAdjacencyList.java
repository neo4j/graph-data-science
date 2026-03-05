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
package org.neo4j.gds.core.compression.varlong;

import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.Arrays;

import static org.neo4j.gds.collections.PageUtil.indexInPage;
import static org.neo4j.gds.collections.PageUtil.pageIndex;
import static org.neo4j.gds.compression.common.BumpAllocator.PAGE_MASK;
import static org.neo4j.gds.compression.common.BumpAllocator.PAGE_SHIFT;
import static org.neo4j.gds.compression.common.BumpAllocator.PAGE_SIZE;

/**
 * A representation of a {@link org.neo4j.gds.core.compression.varlong.CompressedAdjacencyList} that allows
 * for efficient access to the compressed target lists of nodes without decompressing them.
 *
 */
public abstract class CompressedSlicedAdjacencyList {

    final byte[][] compressedPages;
    // The offset in `compressedPages` where the compressed target list for a node starts.
    final HugeLongArray offsets;
    // The degree of a node
    final HugeIntArray degrees;

    public static CompressedSlicedAdjacencyList of(CompressedAdjacencyList compressedAdjacencyList) {
        byte[][] compressedPages = compressedAdjacencyList.pages();
        HugeLongArray offsets = compressedAdjacencyList.offsets();
        HugeIntArray degrees = compressedAdjacencyList.degrees();

        return new WithByteCounting(compressedPages, degrees, offsets);

    }

    private CompressedSlicedAdjacencyList(
        byte[][] compressedPages,
        HugeIntArray degrees,
        HugeLongArray offsets
    ) {
        this.compressedPages = compressedPages;
        this.degrees = degrees;
        this.offsets = offsets;
    }

    /**
     * An overestimate of the total number of bytes used by the compressed pages.
     */
    public long totalPageBytes() {
        return Arrays.stream(this.compressedPages).mapToLong(page -> page.length).sum();
    }

    public long nodeCount() {
        return this.offsets.size();
    }

    public int degree(long nodeId) {
        return this.degrees.get(nodeId);
    }

    public PageSlice newPageSlice() {
        return new PageSlice();
    }

    public static class PageSlice {
        public byte[] page;
        public int offset;
        public int length;
    }

    public abstract boolean initPageSlice(long nodeId, PageSlice slice);

    static final class WithByteCounting extends CompressedSlicedAdjacencyList {

        private WithByteCounting(byte[][] compressedPages, HugeIntArray degrees, HugeLongArray offsets) {
            super(compressedPages, degrees, offsets);
        }

        @Override
        public boolean initPageSlice(long nodeId, PageSlice slice) {
            int degree = this.degrees.get(nodeId);
            if (degree == 0) {
                return false;
            }
            long offset = this.offsets.get(nodeId);
            int pageIndex = pageIndex(offset, PAGE_SHIFT);
            byte[] page = this.compressedPages[pageIndex];
            int indexInPage = indexInPage(offset, PAGE_MASK);

            if (page.length > PAGE_SIZE) {
                // oversize page, we can directly compute the length without looking for the end byte
                slice.page = page;
                slice.offset = indexInPage;
                slice.length = page.length;
                return true;
            }

            slice.page = page;
            slice.offset = indexInPage;
            slice.length = computeCompressedLength(page, indexInPage, degree);

            return true;
        }

        private int computeCompressedLength(byte[] page, int offset, int degree) {
            int indexInPage = offset;
            for (int i = 0; i < degree; i++) {
                // during compression, we mark the last byte of a var long by
                // setting the MSB (sign bit) to 1 and end up with a negative value.
                while (page[indexInPage] >= 0) {
                    indexInPage++;
                }
                // account for the last byte of the var long
                indexInPage++;
            }
            return indexInPage - offset;
        }
    }
}
