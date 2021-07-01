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
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.function.LongPredicate;

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
        int pastPageIdx = -1;

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

                if (pageIdx != pastPageIdx) {
                    ordering[pageIdx] = idx;
                    pageOffsets[idx] = nodeId;
                    pastPageIdx = pageIdx;
                    idx = idx + 1;
                }
            }
        }
        pageOffsets[idx] = offsets.size();

        return ImmutablePageOrdering
            .builder()
            .ordering(ordering)
            .pageOffsets(pageOffsets)
            .build();
    }

    public static <PAGE> int[] reorder(PAGE[] pages, int[] ordering) {
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

    public static HugeLongArray sortOffsets(
        HugeLongArray offsets,
        PageOrdering pageOrdering,
        AllocationTracker tracker
    ) {
        int[] ordering = pageOrdering.ordering();
        long[] pageOffsets = pageOrdering.pageOffsets();

        var newOffsets = HugeLongArray.newArray(offsets.size(), tracker);
        long newIdx = 0;

        for (int sourcePage : ordering) {
            long startIdx = pageOffsets[sourcePage];
            long endIdx = pageOffsets[sourcePage + 1];

            for (long idx = startIdx; idx < endIdx; idx++) {
                newOffsets.set(newIdx++, offsets.get(idx));
            }
        }
        return newOffsets;
    }

    private PageReordering() {}
}
