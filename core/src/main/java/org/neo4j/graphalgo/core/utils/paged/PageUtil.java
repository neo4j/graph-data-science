/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.utils.BitUtil;

public final class PageUtil {

    // 32 KB page size
    private static final int PAGE_SIZE_IN_BYTES = 1 << 15;

    public static int pageSizeFor(int sizeOfElement) {
        assert BitUtil.isPowerOfTwo(sizeOfElement);
        return PAGE_SIZE_IN_BYTES >> Integer.numberOfTrailingZeros(sizeOfElement);
    }

    public static int numPagesFor(long capacity, int pageSize) {
        int pageShift = Integer.numberOfTrailingZeros(pageSize);
        int pageMask = pageSize - 1;
        return numPagesFor(capacity, pageShift, pageMask);
    }

    public static int numPagesFor(long capacity, int pageShift, long pageMask) {
        final long numPages = (capacity + pageMask) >>> pageShift;
        assert numPages <= Integer.MAX_VALUE : "pageSize=" + (pageMask + 1) + " is too small for such as capacity: " + capacity;
        return (int) numPages;
    }

    public static long capacityFor(int numPages, int pageShift) {
        return ((long) numPages) << pageShift;
    }

    public static int pageIndex(long index, int pageShift) {
        return (int) (index >>> pageShift);
    }

    public static int indexInPage(long index, int pageMask) {
        return (int) (index & (long) pageMask);
    }

    public static int indexInPage(long index, long pageMask) {
        return (int) (index & pageMask);
    }

    private PageUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
