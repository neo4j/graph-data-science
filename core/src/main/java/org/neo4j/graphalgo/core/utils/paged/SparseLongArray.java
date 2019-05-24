/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import java.util.Arrays;

public final class SparseLongArray {

    private static final long NOT_FOUND = -1L;

    private static final int PAGE_SHIFT = 12;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);
    private static final long PAGE_SIZE_IN_BYTES = MemoryUsage.sizeOfLongArray(PAGE_SIZE);

    private final long capacity;
    private final long[][] pages;
    private final AllocationTracker tracker;

    public static SparseLongArray newArray(
            long size,
            AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, (int) PAGE_MASK);
        long capacity = PageUtil.capacityFor(numPages, PAGE_SHIFT);
        long[][] pages = new long[numPages][];
        tracker.add(MemoryUsage.shallowSizeOfInstance(SparseLongArray.class));
        tracker.add(MemoryUsage.sizeOfObjectArray(numPages));
        return new SparseLongArray(capacity, pages, tracker);
    }

    private SparseLongArray(long capacity, long[][] pages, AllocationTracker tracker) {
        this.capacity = capacity;
        this.pages = pages;
        this.tracker = tracker;
    }

    public long get(long index) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        long[] page = pages[pageIndex];
        if (page != null) {
            final int indexInPage = indexInPage(index);
            return page[indexInPage];
        }
        return NOT_FOUND;
    }

    public void set(long index, long value) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        long[] page = pages[pageIndex];
        if (page == null) {
            page = allocateNewPage();
            pages[pageIndex] = page;
        }
        page[indexInPage] = value;
    }

    public boolean contains(long index) {
        assert index < capacity;
        final int pageIndex = pageIndex(index);
        long[] page = pages[pageIndex];
        if (page != null) {
            final int indexInPage = indexInPage(index);
            return page[indexInPage] != NOT_FOUND;
        }
        return false;
    }

    private int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }

    private long[] allocateNewPage() {
        tracker.add(PAGE_SIZE_IN_BYTES);
        final long[] page = new long[PAGE_SIZE];
        Arrays.fill(page, NOT_FOUND);
        return page;
    }
}
