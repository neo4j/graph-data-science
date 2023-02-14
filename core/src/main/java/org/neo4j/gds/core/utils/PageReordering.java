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
package org.neo4j.gds.core.utils;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.compression.common.BumpAllocator;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.function.LongPredicate;

public final class PageReordering {

    private static final long ZERO_DEGREE_OFFSET = 0;

    @ValueClass
    public interface PageOrdering {
        /**
         * Represents the order in which pages
         * occur according to the offsets. Only
         * the first occurrence of a page is being
         * recorded.
         */
        int[] distinctOrdering();

        /**
         * Represents the order of the indexes at which
         * pages occur according to the offsets.
         * Since a page can occur multiple times within
         * a consecutive range of offsets, the index of
         * it's first occurrence can be added multiple times.
         *
         * The size of this array can be larger than the
         * total number of pages.
         */
        int[] reverseOrdering();

        /**
         * Represents the start and end indexes within
         * the offsets where a page starts or ends. The
         * length of this array is determined by the length
         * of {@link PageOrdering#reverseOrdering}.
         */
        long[] pageOffsets();

        /**
         * The actual array length of {@link PageOrdering#reverseOrdering}.
         */
        int length();

        @TestOnly
        default int[] shrinkToFitReverseOrdering() {
            return Arrays.copyOf(reverseOrdering(), length());
        }

        @TestOnly
        default long[] shrinkToFitPageOffsets() {
            return Arrays.copyOf(pageOffsets(), length() + 1);
        }
    }

    /**
     * This method aligns the given pages and offsets with the node id space.
     * Pages and offsets are changed in-place in O(nodeCount) time.
     *
     * Reordering happens in three steps:
     *
     * <ol>
     *     <li>
     *         The offsets are scanned to detect the current page ordering
     *         and the start and end indexes of a page within the offsets.
     *     </li>
     *     <li>
     *         The pages are swapped to end up in order.
     *     </li>
     *     <li>
     *         The offsets are rewritten to contain the new page id,
     *         but the same index within the page.
     *     </li>
     * </ol>
     *
     * Note that only offsets for nodes with degree &gt; 0 are being rewritten.
     * Nodes with degree = 0 will have offset = 0.
     *
     * <pre>
     * Example for page size = 8
     *
     * Input:
     *
     * pages    [  r  g  b  s ]
     * offsets  [ 16, 18, 22, 0, 3, 6, 24, 28, 30, 8, 13, 15 ]
     *
     * Lookup:
     * node 0 -&gt; offset 16 -&gt; page id 2 -&gt; index in page 0 -&gt; page b
     * node 4 -&gt; offset  3 -&gt; page id 0 -&gt; index in page 3 -&gt; page r
     *
     * Output:
     *
     * page ordering     [  2  0  3  1 ]
     * ordered pages     [  b  r  s  g ]
     * rewritten offsets [  0, 2, 6, 8, 11, 14, 16, 20, 22, 24, 29, 31 ]
     *
     * Lookup:
     * node 0 -&gt; offset  0 -&gt; page id 0 -&gt; index in page 0 -&gt; page b
     * node 4 -&gt; offset 11 -&gt; page id 1 -&gt; index in page 3 -&gt; page r
     * </pre>
     */
    public static <PAGE> void reorder(PAGE[] pages, HugeLongArray offsets, HugeIntArray degrees) {
        var ordering = ordering(
            offsets,
            nodeId -> degrees.get(nodeId) > 0,
            pages.length,
            BumpAllocator.PAGE_SHIFT
        );
        reorder(pages, ordering.distinctOrdering());
        rewriteOffsets(offsets, ordering, node -> degrees.get(node) > 0, BumpAllocator.PAGE_SHIFT);
    }

    static PageOrdering ordering(
        HugeLongArray offsets,
        LongPredicate nodeFilter,
        int pageCount,
        int pageShift
    ) {
        var cursor = offsets.initCursor(offsets.newCursor());

        var pageOffsets = new LongArrayList(pageCount + 1);
        var ordering = new IntArrayList(pageCount);
        int[] distinctOrdering = new int[pageCount];
        int[] reverseDistinctOrdering = new int[pageCount];

        int orderedIdx = 0;
        int prevPageIdx = -1;
        var seenPages = new BitSet(pageCount);

        while (cursor.next()) {
            var offsetArray = cursor.array;
            var limit = cursor.limit;
            var base = cursor.base;

            for (int i = cursor.offset; i < limit; i++) {
                long nodeId = base + i;
                // typically, the nodeFilter would return false for unconnected nodes
                if (!nodeFilter.test(nodeId)) {
                    continue;
                }

                var offset = offsetArray[i];
                var pageIdx = (int) (offset >>> pageShift);

                if (pageIdx != prevPageIdx) {
                    if (!seenPages.getAndSet(pageIdx)) {
                        distinctOrdering[orderedIdx] = pageIdx;
                        reverseDistinctOrdering[pageIdx] = orderedIdx;
                        orderedIdx = orderedIdx + 1;
                    }
                    ordering.add(reverseDistinctOrdering[pageIdx]);
                    pageOffsets.add(nodeId);
                    prevPageIdx = pageIdx;
                }
            }
        }
        pageOffsets.add(offsets.size());

        return ImmutablePageOrdering
            .builder()
            .distinctOrdering(distinctOrdering)
            .reverseOrdering(ordering.buffer)
            .length(ordering.elementsCount)
            .pageOffsets(pageOffsets.buffer)
            .build();
    }

    static <PAGE> int[] reorder(PAGE[] pages, int[] ordering) {
        PAGE tempPage;
        var swaps = new int[pages.length];
        Arrays.setAll(swaps, i -> -i - 1);

        for (int targetIdx = 0; targetIdx < ordering.length; targetIdx++) {
            int sourceIdx = ordering[targetIdx];

            var swapTargetIdx = swaps[targetIdx];
            assert (swapTargetIdx < 0): "target page has already been set";

            // If swapSourceIdx > 0, the page has been swapped already
            // and we need to follow that index until we find a free slot.
            int swapSourceIdx = sourceIdx;

            while (swaps[swapSourceIdx] >= 0) {
                swapSourceIdx = swaps[swapSourceIdx];
            }

            assert (swaps[swapSourceIdx] == -sourceIdx - 1): "source page has already been moved";

            if (swapSourceIdx == targetIdx) {
                swaps[targetIdx] = sourceIdx;
            } else {
                tempPage = pages[targetIdx];
                pages[targetIdx] = pages[swapSourceIdx];
                pages[swapSourceIdx] = tempPage;

                swaps[targetIdx] = sourceIdx;
                swaps[swapSourceIdx] = swapTargetIdx;
            }
        }

        return swaps;
    }

    static void rewriteOffsets(
        HugeLongArray offsets,
        PageOrdering pageOrdering,
        LongPredicate nodeFilter,
        int pageShift
    ) {
        // the pageShift number of lower bits are set, the higher bits are empty.
        long pageMask = (1L << pageShift) - 1;
        var pageOffsets = pageOrdering.pageOffsets();
        try (var cursor = offsets.newCursor()) {

            var ordering = pageOrdering.reverseOrdering();
            var length = pageOrdering.length();

            for (int i = 0; i < length; i++) {
                // higher bits in pageId part are set to the pageId
                long newPageId = ((long) ordering[i]) << pageShift;

                long startIdx = pageOffsets[i];
                long endIdx = pageOffsets[i + 1];

                offsets.initCursor(cursor, startIdx, endIdx);
                while (cursor.next()) {
                    var array = cursor.array;
                    var limit = cursor.limit;
                    long baseNodeId = cursor.base;
                    for (int j = cursor.offset; j < limit; j++) {
                        array[j] = nodeFilter.test(baseNodeId + j)
                            ? (array[j] & pageMask) | newPageId
                            : ZERO_DEGREE_OFFSET;
                    }
                }
            }
        }
    }

    private PageReordering() {}
}
