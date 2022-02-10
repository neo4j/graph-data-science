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

import java.lang.reflect.Array;
import java.util.Arrays;

public final class HugeSparseObjectArrayList<T> {

    public interface LongObjectConsumer<T> {
        void consume(long index, T value);
    }

    private final Class<T> clazz;

    private static final int PAGE_SHIFT = 12;

    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;

    private static final int PAGE_MASK = PAGE_SIZE - 1;

    private T[][] pages;

    private final T defaultValue;

    public static <T> HugeSparseObjectArrayList<T> of(T defaultValue, Class<T> clazz) {
        return new HugeSparseObjectArrayList<>(defaultValue, 0, clazz);
    }

    public static <T> HugeSparseObjectArrayList<T> of(T defaultValue, long initialCapacity, Class<T> clazz) {
        return new HugeSparseObjectArrayList<>(defaultValue, initialCapacity, clazz);
    }

    @SuppressWarnings("unchecked")
    private HugeSparseObjectArrayList(T defaultValue, long initialCapacity, Class<T> clazz) {
        int pageCount = PageUtil.pageIndex(initialCapacity, PAGE_SHIFT);
        // 😢 java
        var arrayClazz = (Class<T[]>) (Array.newInstance(clazz, 0)).getClass();
        this.pages = (T[][]) Array.newInstance(arrayClazz, pageCount);
        this.defaultValue = defaultValue;
        this.clazz = clazz;
    }

    public void forAll(LongObjectConsumer<T> consumer) {
        T[][] longs = this.pages;
        for (int pageIndex = 0; pageIndex < longs.length; pageIndex++) {
            T[] page = longs[pageIndex];
            if (page == null) {
                continue;
            }
            for (int indexInPage = 0; indexInPage < page.length; indexInPage++) {
                T value = page[indexInPage];
                if (value.equals(defaultValue)) {
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

    public T get(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        if (pageIndex < pages.length) {
            T[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage];
            }
        }
        return defaultValue;
    }

    public boolean contains(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        if (pageIndex < pages.length) {
            T[] page = pages[pageIndex];
            if (page != null) {
                int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
                return !page[indexInPage].equals(defaultValue);
            }
        }
        return false;
    }

    public void set(long index, T value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        getPage(pageIndex)[indexInPage] = value;
    }

    public boolean setIfAbsent(long index, T value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        var page = getPage(pageIndex);
        T currentValue = page[indexInPage];
        if (currentValue.equals(defaultValue)) {
            page[indexInPage] = value;
            return true;
        }
        return false;
    }

    private void grow(int minNewSize) {
        if (minNewSize <= pages.length) {
            return;
        }

        var newSize = HugeArrays.oversizeInt(minNewSize, MemoryUsage.BYTES_OBJECT_REF);
        this.pages = Arrays.copyOf(this.pages, newSize);
    }

    private T[] getPage(int pageIndex) {
        if (pageIndex >= pages.length) {
            grow(pageIndex + 1);
        }
        T[] page = pages[pageIndex];
        if (page == null) {
            page = allocateNewPage(pageIndex);
        }
        return page;
    }

    @SuppressWarnings("unchecked")
    private T[] allocateNewPage(int pageIndex) {
        var page = (T[]) Array.newInstance(clazz, PAGE_SIZE);
        Arrays.fill(page, defaultValue);
        this.pages[pageIndex] = page;
        return page;
    }

    public DrainingIterator<T[]> drainingIterator() {
        return new DrainingIterator<>(pages, PAGE_SIZE);
    }
}
