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
package org.neo4j.gds.collections.arraylist;

import org.neo4j.gds.collections.DrainingIterator;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.mem.HugeArrays;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Arrays;

public final class HugeSparseIntArrayList {

    interface LongIntConsumer {
        void consume(long index, int value);
    }

    private static final int PAGE_SHIFT = 12;

    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;

    private static final int PAGE_MASK = PAGE_SIZE - 1;

    private int[][] pages;

    private final int defaultValue;

    public static HugeSparseIntArrayList of(int defaultValue) {
        return new HugeSparseIntArrayList(defaultValue, 0);
    }

    public static HugeSparseIntArrayList of(int defaultValue, long initialCapacity) {
        return new HugeSparseIntArrayList(defaultValue, initialCapacity);
    }

    private HugeSparseIntArrayList(int defaultValue, long initialCapacity) {
        int pageCount = PageUtil.pageIndex(initialCapacity, PAGE_SHIFT);
        this.pages = new int[pageCount][];
        this.defaultValue = defaultValue;
    }

    public void forAll(LongIntConsumer consumer) {
        int[][] longs = this.pages;
        for (int pageIndex = 0; pageIndex < longs.length; pageIndex++) {
            int[] page = longs[pageIndex];
            if (page == null) {
                continue;
            }
            for (int indexInPage = 0; indexInPage < page.length; indexInPage++) {
                int value = page[indexInPage];
                if (value == defaultValue) {
                    continue;
                }

                long index = ((long) pageIndex << PAGE_SHIFT) | (long) indexInPage;

                consumer.consume(index, value);
            }
        }
    }

    public long capacity() {
        int numPages = pages.length;
        return ((long) numPages) << PAGE_SHIFT;
    }

    public int get(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        if (pageIndex < pages.length) {
            int[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage];
            }
        }
        return defaultValue;
    }

    public boolean contains(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        if (pageIndex < pages.length) {
            int[] page = pages[pageIndex];
            if (page != null) {
                int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
                return page[indexInPage] != defaultValue;
            }
        }
        return false;
    }

    public void set(long index, int value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        getPage(pageIndex)[indexInPage] = value;
    }

    public boolean setIfAbsent(long index, int value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        var page = getPage(pageIndex);
        long currentValue = page[indexInPage];
        if (currentValue == defaultValue) {
            page[indexInPage] = value;
            return true;
        }
        return false;
    }

    public void addTo(long index, int value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        int[] page = getPage(pageIndex);
        page[indexInPage] += value;
    }

    public DrainingIterator<int[]> drainingIterator() {
        return new DrainingIterator<>(pages, PAGE_SIZE);
    }

    private void grow(int minNewSize) {
        if (minNewSize <= pages.length) {
            return;
        }

        var newSize = HugeArrays.oversizeInt(minNewSize, MemoryUsage.BYTES_OBJECT_REF);
        this.pages = Arrays.copyOf(this.pages, newSize);
    }

    private int[] getPage(int pageIndex) {
        if (pageIndex >= pages.length) {
            grow(pageIndex + 1);
        }
        int[] page = pages[pageIndex];
        if (page == null) {
            page = allocateNewPage(pageIndex);
        }
        return page;
    }

    private int[] allocateNewPage(int pageIndex) {
        var page = new int[PAGE_SIZE];
        Arrays.fill(page, defaultValue);
        this.pages[pageIndex] = page;
        return page;
    }

}
