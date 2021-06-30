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

import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Arrays;

public final class PageReordering {

    public static int[] ordering(HugeLongArray offsets, int pageCount, int pageShift) {
        // TODO implement using HLA cursors
        var offsetArray = offsets.toArray();

        int[] pageOffsets = new int[pageCount];
        int[] pageIndexes = new int[pageCount];

        int idx = 0;
        int pastPageIdx = -1;

        for (int i = 0; i < offsetArray.length; i++) {
            var offset = offsetArray[i];
            var pageIdx = (int) offset >>> pageShift;

            if (pageIdx != pastPageIdx) {
                pageOffsets[idx] = i;
                pageIndexes[idx] = pageIdx;
                pastPageIdx = pageIdx;
                idx = idx + 1;
            }
        }

        return IndirectSort.mergesort(
            0,
            pageIndexes.length,
            new IndirectComparator.AscendingIntComparator(pageIndexes)
        );
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

    public static void rewriteOffsets(HugeLongArray offsets, int[] ordering, int pageShift) {

    }

    private PageReordering() {}
}
