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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class PageReorderingTest {

    @Test
    void testOrdering() {
        var pageShift = 3;

        var offsets = HugeLongArray.of(
            8, 13, 15, // page 1
            0, 3, 6,   // page 0
            16, 18, 22 // page 2
        );

        var ordering = PageReordering.ordering(offsets, 3, pageShift);

        assertThat(ordering).isEqualTo(new int[]{1, 0, 2});
    }

    @Test
    void testReordering() {
        var pageShift = 3;

        var offsets = HugeLongArray.of(
            8, 13, 15, // page 1
            0, 3, 6,   // page 0
            16, 18, 22 // page 2
        );

        var ordering = PageReordering.ordering(offsets, 3, pageShift);

        var page0 = new int[0];
        var page1 = new int[0];
        var page2 = new int[0];

        var pages = new int[][]{page1, page0, page2};

        PageReordering.reorder(pages, ordering);

        assertThat(pages).isEqualTo(new int[][]{page0, page1, page2});
    }

    @Test
    void testRewriteOffsets() {
        var pageShift = 3;

        var offsets = HugeLongArray.of(
            8, 13, 15, // page 1
            0, 3, 6,   // page 0
            16, 18, 22 // page 2
        );

        var ordering = PageReordering.ordering(offsets, 3, pageShift);

        PageReordering.rewriteOffsets(offsets, ordering, pageShift);

        assertThat(offsets.toArray()).isEqualTo(new long[] {0, 5, 7, 8, 11, 14, 16, 18, 22});
    }

}
