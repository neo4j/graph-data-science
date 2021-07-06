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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.PageUtil;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;


/**
 * Input
 *
 * pages    [  r  g  b  s ]
 * offsets  [ 16, 18, 22, 0, 3, 6, 24, 28, 30, 8, 13, 15 ]
 *
 * node 0 -> offset 16 -> page id 2 -> page b
 * node 3 -> offset  0 -> page id 0 -> page r
 *
 * Output
 *
 * ordering [  2  0  3  1 ]
 * pages    [  b  r  s  g ]
 * offset   [  0, 2, 6, 8, 11, 14, 16, 20, 22, 24, 29, 31 ]
 *
 * node 0 -> offset 0 -> page id 0 -> page b
 * node 3 -> offset 8 -> page id 1 -> page r
 */
public final class PageReordering {

    @ValueClass
    public interface PageOrdering {
        int[] ordering();

        long[] pageOffsets();
    }

    public static PageOrdering ordering(
        HugeLongArray offsets,
        LongPredicate nodeFilter,
        int pageCount,
        int pageShift
    ) {
        var cursor = offsets.initCursor(offsets.newCursor());

        long[] pageOffsets = new long[pageCount + 1];
        int[] ordering = new int[pageCount];

        int idx = 0;
        int prevPageIdx = -1;

        while (cursor.next()) {
            var array = cursor.array;
            var limit = cursor.limit;
            var base = cursor.base;

            for (int i = cursor.offset; i < limit; i++) {
                var nodeId = base + i;
                // typically, the nodeFilter would return false for unconnected nodes
                if (!nodeFilter.test(nodeId)) {
                    continue;
                }

                var offset = array[i];
                var pageIdx = (int) (offset >>> pageShift);

                if (pageIdx != prevPageIdx) {
                    ordering[idx] = pageIdx;
                    pageOffsets[idx] = nodeId;
                    prevPageIdx = pageIdx;
                    idx = idx + 1;
                }
            }
        }
        pageOffsets[idx] = offsets.size();


        // validation
        var copyOfOrdering = Arrays.copyOf(ordering, ordering.length);
        Arrays.sort(copyOfOrdering);
        for (int i = 0; i < copyOfOrdering.length; i++) {
            if (i != copyOfOrdering[i]) {
                throw new IllegalStateException(Arrays.toString(copyOfOrdering));
            }
        }

        var pageIds = new ArrayList<>();

        for (int i = 0; i < pageOffsets.length - 1; i++) {
            long startIdx = pageOffsets[i];
            long endIdx = pageOffsets[i + 1];

            int pageId = (int) (offsets.get(startIdx) >>> pageShift);
            pageIds.add(pageId);
            for (long j = startIdx + 1; j < endIdx; j++) {
                if (nodeFilter.test(j)) {
                    int pId = (int) (offsets.get(j) >>> pageShift);
                    if (pageId != pId) {
                        throw new IllegalStateException("invalid page id within range");
                    }
                }
            }
        }

        var dedupPageIds = pageIds.stream().distinct().collect(Collectors.toList());

        if (pageIds.size() != dedupPageIds.size()) {
            throw new IllegalStateException();
        }

        return ImmutablePageOrdering
            .builder()
            .ordering(ordering)
            .pageOffsets(pageOffsets)
            .build();
    }

    public static <PAGE> int[] reorder(PAGE[] pages, int[] ordering) {

        PAGE[] pagesCopy = Arrays.copyOf(pages, pages.length);

        PAGE tempPage;
        var swaps = new int[pages.length];
        Arrays.setAll(swaps, i -> -i - 1);

        int[] pageLengths = new int[pages.length];

        for (int i = 0; i < pages.length; i++) {
            pageLengths[i] = Array.getLength(pages[i]);
        }

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

        // validation
        for (int i = 0; i < pages.length; i++) {
            var sourcePage = pagesCopy[ordering[i]];
            var targetPage = pages[i];

            if (sourcePage != targetPage) {
                throw new IllegalStateException("ordering violation");
            }
        }

        for (int i = 0; i < ordering.length; i++) {
            if (swaps[i] != ordering[i]) {
                throw new IllegalStateException("swap violation");
            }
        }

        return swaps;
    }

    public static void rewriteOffsets(
        HugeLongArray offsets,
        PageOrdering pageOrdering,
        LongPredicate nodeFilter,
        int pageShift
    ) {
        // the pageShift number of lower bits are set, the higher bits are empty.
        long pageMask = (1L << pageShift) - 1;
        var pageOffsets = pageOrdering.pageOffsets();
        var cursor = offsets.newCursor();

        for (int pageId = 0; pageId < pageOffsets.length - 1; pageId++) {
            // higher bits in pageId part are set to the pageId
            long newPageId = ((long) pageId) << pageShift;

            long startIdx = pageOffsets[pageId];
            long endIdx = pageOffsets[pageId + 1];

            offsets.initCursor(cursor, startIdx, endIdx);
            while (cursor.next()) {
                var array = cursor.array;
                var limit = cursor.limit;
                long baseNodeId = cursor.base;
                for (int i = cursor.offset; i < limit; i++) {
                    long oldOffset = array[i];

                    long nodeId = baseNodeId + i;
                    long newOffset = nodeFilter.test(nodeId)
                        ? (oldOffset & pageMask) | newPageId
                        : -1L;
                    array[i] = newOffset;


                    // debug only
                    if (!nodeFilter.test(nodeId)) {
                        continue;
                    }

                    if (PageUtil.indexInPage(oldOffset, pageMask) != PageUtil.indexInPage(newOffset, pageMask)) {
                        throw new IllegalStateException("Changed the index into the page: nodeId=" + nodeId + " offset=" + oldOffset + " new offset=" + newOffset + " old iip=" + PageUtil
                            .indexInPage(oldOffset, pageMask) + " new iip=" + PageUtil.indexInPage(
                            newOffset,
                            pageMask
                        ));
                    }

                    if (PageUtil.pageIndex(newOffset, pageShift) != pageId) {
                        throw new IllegalStateException("Changed to the wrong pageId: nodeId=" + nodeId + " offset=" + newOffset + " offset pid=" + PageUtil
                            .pageIndex(newOffset, pageShift) + " expected pid=" + pageId);
                    }


                    long expectedSourcePageId = pageOrdering.ordering()[pageId];
                    long oldPageId = PageUtil.pageIndex(oldOffset, pageShift);

                    if (oldPageId != expectedSourcePageId) {
                        throw new IllegalStateException("Changed from the wrong pageId: nodeId=" + nodeId + " offset=" + oldOffset + " offset pid=" + oldPageId + " expected pid=" + expectedSourcePageId);
                    }
                }
            }
        }

        // validation
        var nOffsets = offsets.toArray();

        var prevPageId = -1;

        for (int nodeId = 0; nodeId < nOffsets.length; nodeId++) {
            if (!nodeFilter.test(nodeId)) {
                continue;
            }
            long nOffset = nOffsets[nodeId];
            int newPageId = (int) (nOffset >>> pageShift);

            if (newPageId < prevPageId) {
                throw new IllegalStateException();

            }
            prevPageId = newPageId;

            var startIdx = pageOffsets[newPageId];
            var endIdx = pageOffsets[newPageId + 1];

            if (nodeId < startIdx || nodeId >= endIdx) {
                throw new IllegalStateException("node id = " + nodeId + " is out of range[" + startIdx + ", " + endIdx + "]");
            }
        }
    }

    private PageReordering() {}
}
