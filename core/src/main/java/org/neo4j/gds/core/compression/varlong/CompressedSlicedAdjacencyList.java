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

import org.neo4j.gds.collections.cursor.HugeCursorSupport;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.HugeMergeSort;
import org.neo4j.gds.core.utils.paged.HugeSerialIndirectMergeSort;

import java.util.Arrays;

import static org.neo4j.gds.collections.PageUtil.indexInPage;
import static org.neo4j.gds.collections.PageUtil.pageIndex;

/**
 * A representation of a {@link org.neo4j.gds.core.compression.varlong.CompressedAdjacencyList} that allows
 * for efficient access to the compressed target lists of nodes without decompressing them.
 *
 * <pre>
 * offsets          = [42,  0, 31, 22]
 * forward_indexes  = [ 3,  0,  2,  1]
 * sorted_offsets   = [ 0, 22, 31, 42]
 *
 * node id 0:   offsets[0] = 42
 * forward_indexes[0]      =  3
 * sorted_offsets[3 + 1]   =  -1 (3 + 1 >= len)
 * to_offset               = 42 + page.len - index_in_page(page)
 *
 * node id 1:   offsets[1] =  0
 * forward_indexes[1]      =  0
 * sorted_offsets[0 + 1]   = 22
 *
 * node id 2:   offsets[2] = 31
 * forward_indexes[2]      =  2
 * sorted_offsets[2 + 1]   = 42
 *
 * node id 3:   offsets[3] = 22
 * forward_indexes[3]      =  1
 * sorted_offsets[1 + 1]   = 31
 * </pre>
 */
public final class CompressedSlicedAdjacencyList {

    static final long ZERO_DEGREE = -1;

    private final byte[][] compressedPages;
    // The offset in `compressedPages` where the compressed target list for a node starts.
    private final HugeLongArray offsets;
    // The degree of a node
    private final HugeIntArray degrees;
    // Offsets sorted in ascending order.
    private final HugeLongArray sortedOffsets;
    // Maps node id to its index in sortedOffsets
    private final HugeLongArray forwardIndexes;
    // well ..
    private long nodeCount;

    public static CompressedSlicedAdjacencyList of(CompressedAdjacencyList compressedAdjacencyList, Concurrency concurrency) {
        byte[][] compressedPages = compressedAdjacencyList.pages;
        HugeLongArray offsets = compressedAdjacencyList.offsets;
        HugeIntArray degrees = compressedAdjacencyList.degrees;

        // O(3*n) space
        HugeLongArray sortedOffsets = offsets.copyOf(offsets.size());
        HugeLongArray sortedIndexes = HugeLongArray.newArray(offsets.size());
        HugeLongArray forwardIndexes = HugeLongArray.newArray(offsets.size());

        // 1. sort the offsets ascending O(nlog(n)); use sortedIndexes as a temporary array
        HugeMergeSort.sort(sortedOffsets, concurrency, sortedIndexes);
        // 2. sort the indexes (node ids) according to offset order O(nlog(n))
        fillWithIndex(sortedIndexes);
        HugeSerialIndirectMergeSort.sort(sortedIndexes, sortedIndexes.size(), node -> {
            long offset = offsets.get(node);
            // 0-degree nodes have offset set to 0.
            // We need to make sure that if node id 0 has a degree > 0, it ends
            // up at the end of all 0-degree nodes in the sort order. This allows
            // for less complexity in the endOffset computation.
            return offset + degrees.get(node) > 0 ? 1 : 0;
        }, forwardIndexes);
        // 3. map each node id to its index in the offset order
        forwardIndexes = buildForwardIndex(sortedIndexes, forwardIndexes);

        return new CompressedSlicedAdjacencyList(
            compressedPages,
            degrees,
            offsets,
            sortedOffsets,
            forwardIndexes
        );
    }

    private CompressedSlicedAdjacencyList(
        byte[][] compressedPages,
        HugeIntArray degrees,
        HugeLongArray offsets,
        HugeLongArray sortedOffsets,
        HugeLongArray forwardIndexes
    ) {
        this.compressedPages = compressedPages;
        this.degrees = degrees;
        this.offsets = offsets;
        this.sortedOffsets = sortedOffsets;
        this.forwardIndexes = forwardIndexes;
        this.nodeCount = offsets.size();
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

    public boolean initPageSlice(long nodeId, PageSlice slice) {
        long startOffset = startOffset(nodeId);
        if (startOffset == ZERO_DEGREE) {
            return false;
        }
        int startPageIndex = pageIndex(startOffset, BumpAllocator.PAGE_SHIFT);
        byte[] page = this.compressedPages[startPageIndex];
        int startIndexInPage = indexInPage(startOffset, BumpAllocator.PAGE_MASK);
        long endOffset = endOffset(nodeId);
        int endIndexInPage = indexInPage(endOffset, BumpAllocator.PAGE_MASK);
        if (endIndexInPage == 0) {
            // Next offset starts on next page, we need to set the slice to
            // end at the end of the page to also cover oversize pages.
            endIndexInPage = page.length;
        }

        slice.page = page;
        slice.offset = startIndexInPage;
        slice.length = endIndexInPage - startIndexInPage;

        return true;
    }

    public PageSlice newPageSlice() {
        return new PageSlice();
    }

    public static class PageSlice {
        public byte[] page;
        public int offset;
        public int length;
    }

    long startOffset(long nodeId) {
        if (this.degrees.get(nodeId) == 0) {
            return ZERO_DEGREE;
        }
        return this.offsets.get(nodeId);
    }

    long endOffset(long nodeId) {
        if (this.degrees.get(nodeId) == 0) {
            return ZERO_DEGREE;
        }
        // position of node id in sorted offsets
        long fromIndex = this.forwardIndexes.get(nodeId);
        long toIndex = fromIndex + 1;

        if (toIndex >= this.nodeCount) {
            // fromIndex refers to the last offset on the last page.
            // We do not know the length of that slice, so we need
            // to search its end.
            return findEndOfSlice(fromIndex);
        }

        return this.sortedOffsets.get(toIndex);
    }

    private long findEndOfSlice(long fromIndex) {
        int pageIndex = pageIndex(this.offsets.get(fromIndex), BumpAllocator.PAGE_SHIFT);
        int indexInPage = indexInPage(this.offsets.get(fromIndex), BumpAllocator.PAGE_MASK);
        byte[] page = this.compressedPages[pageIndex];

        long endOffset = sortedOffsets.get(fromIndex);
        for (; indexInPage < page.length - 1; indexInPage++, endOffset++) {
            // During compression, we mark the last byte of a var long by
            // setting the HSB to 1 and end up with a negative value.
            // If that value is followed by 0, we know that we have reached
            // the end of the slice.
            if (page[indexInPage] < 0 && page[indexInPage + 1] == 0) {
                return endOffset + 1;
            }
        }
        return endOffset + 1;
    }

    /**
     * Initialize the array with identity values.
     */
    private static void fillWithIndex(HugeLongArray array) {
        var cursor = array.initCursor(array.newCursor());
        while (cursor.next()) {
            long[] offsetArray = cursor.array;
            int limit = cursor.limit;
            long base = cursor.base;

            for (int i = cursor.offset; i < limit; i++) {
                offsetArray[i] = base + i;
            }
        }
    }

    /**
     * Maps node ids to their index in offset sort order.
     */
    private static HugeLongArray buildForwardIndex(
        HugeCursorSupport<long[]> sortedIndexes,
        HugeLongArray forwardIndexes
    ) {
        var cursor = sortedIndexes.initCursor(sortedIndexes.newCursor());
        while (cursor.next()) {
            long[] offsetArray = cursor.array;
            int limit = cursor.limit;
            long base = cursor.base;

            for (int i = cursor.offset; i < limit; i++) {
                forwardIndexes.set(offsetArray[i], base + i);
            }
        }
        return forwardIndexes;
    }
}
