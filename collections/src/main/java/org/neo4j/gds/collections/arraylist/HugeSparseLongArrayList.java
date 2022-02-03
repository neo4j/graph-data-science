package org.neo4j.gds.collections.arraylist;

import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.mem.HugeArrays;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Arrays;

final class HugeSparseLongArrayList {

    interface LongLongConsumer {
        void consume(long index, long value);
    }

    private static final int PAGE_SHIFT = 12;

    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;

    private static final int PAGE_MASK = PAGE_SIZE - 1;

    private long[][] pages;

    private final long defaultValue;

    public static HugeSparseLongArrayList of(long defaultValue) {
        return new HugeSparseLongArrayList(defaultValue, 0);
    }

    public static HugeSparseLongArrayList of(long defaultValue, long initialCapacity) {
        return new HugeSparseLongArrayList(defaultValue, initialCapacity);
    }

    private HugeSparseLongArrayList(long defaultValue, long initialCapacity) {
        int pageCount = PageUtil.pageIndex(initialCapacity, PAGE_SHIFT);
        this.pages = new long[pageCount][];
        this.defaultValue = defaultValue;
    }

    public void forAll(LongLongConsumer consumer) {
        long[][] longs = this.pages;
        for (int pageIndex = 0; pageIndex < longs.length; pageIndex++) {
            long[] page = longs[pageIndex];
            if (page == null) {
                continue;
            }
            for (int indexInPage = 0; indexInPage < page.length; indexInPage++) {
                long value = page[indexInPage];
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

    public long get(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        if (pageIndex < pages.length) {
            long[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage];
            }
        }
        return defaultValue;
    }

    public boolean contains(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        if (pageIndex < pages.length) {
            long[] page = pages[pageIndex];
            if (page != null) {
                int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
                return page[indexInPage] != defaultValue;
            }
        }
        return false;
    }

    public void set(long index, long value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        getPage(pageIndex)[indexInPage] = value;
    }

    public boolean setIfAbsent(long index, long value) {
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

    public void addTo(long index, long value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        long[] page = getPage(pageIndex);
        page[indexInPage] += value;
    }

    private void grow(int minNewSize) {
        if (minNewSize <= pages.length) {
            return;
        }

        var newSize = HugeArrays.oversizeInt(minNewSize, MemoryUsage.BYTES_OBJECT_REF);
        this.pages = Arrays.copyOf(this.pages, newSize);
    }

    private long[] getPage(int pageIndex) {
        if (pageIndex >= pages.length) {
            grow(pageIndex + 1);
        }
        long[] page = pages[pageIndex];
        if (page == null) {
            page = allocateNewPage(pageIndex);
        }
        return page;
    }

    private long[] allocateNewPage(int pageIndex) {
        var page = new long[PAGE_SIZE];
        Arrays.fill(page, defaultValue);
        this.pages[pageIndex] = page;
        return page;
    }

}
