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
package org.neo4j.gds.mem;

import static org.neo4j.gds.ArrayUtil.oversizeHuge;

public final class HugeArrays {

    // Arrays larger than this have a higher risk of triggering a full GC
    // Prevents full GC more often as not so much consecutive memory is allocated in one go as
    // compared to a page shift of 30 or 32. See https://github.com/neo4j-contrib/neo4j-graph-algorithms/pull/859#discussion_r272262734.
    public static final int MAX_ARRAY_LENGTH = 1 << 28;

    public static final int PAGE_SHIFT = 14;
    public static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = PAGE_SIZE - 1;

    public static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    public static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }

    public static int exclusiveIndexOfPage(long index) {
        return 1 + (int) ((index - 1L) & PAGE_MASK);
    }

    public static long indexFromPageIndexAndIndexInPage(int pageIndex, int indexInPage) {
        return (pageIndex << PAGE_SHIFT) | indexInPage;
    }

    public static int numberOfPages(long capacity) {
        final long numPages = (capacity + PAGE_MASK) >>> PAGE_SHIFT;
        assert numPages <= Integer.MAX_VALUE : "pageSize=" + (PAGE_SIZE) + " is too small for capacity: " + capacity;
        return (int) numPages;
    }

    public static int oversizeInt(int minTargetSize, int bytesPerElement) {
        return Math.toIntExact(oversizeHuge(minTargetSize, bytesPerElement));
    }


    private HugeArrays() {
        throw new UnsupportedOperationException("No instances");
    }
}
