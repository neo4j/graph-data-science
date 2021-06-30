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

    public static <PAGE> void reorder(PAGE[] pages, int[] ordering) {

    }

    public static void rewriteOffsets(HugeLongArray offsets, int[] ordering, int pageShift) {

    }

    private PageReordering() {}
}
