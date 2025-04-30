package org.neo4j.gds.collections.hsl;

import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.collections.DrainingIterator;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.mem.Estimate;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.stream.Stream;


public final class HugeSparseObjectList<E> {
    private static final int PAGE_SHIFT = 12;

    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;

    private static final int PAGE_MASK = PAGE_SIZE - 1;

    private static final long PAGE_SIZE_IN_BYTES = Estimate.sizeOfLongArray(PAGE_SIZE);
    private final Class<E> clazz;

    private E[][] pages;

    private final E defaultValue;

    public HugeSparseObjectList(E defaultValue, long initialCapacity, Class<E> clazz) {
        this.clazz = clazz;
        int numPages = PageUtil.pageIndex(initialCapacity, PAGE_SHIFT);
        this.pages = (E[][]) Array.newInstance(clazz, numPages, PAGE_SIZE);
        this.defaultValue = defaultValue;
    }

    public long capacity() {
        int numPages = pages.length;
        return ((long) numPages) << PAGE_SHIFT;
    }

    public E get(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        if (pageIndex < pages.length) {
            E[] page = pages[pageIndex];
            if (page != null) {
                return page[indexInPage];
            }
        }
        return defaultValue;
    }

    public boolean contains(long index) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        if (pageIndex < pages.length) {
            E[] page = pages[pageIndex];
            if (page != null) {
                int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
                return !page[indexInPage].equals(defaultValue);
            }
        }
        return false;
    }

    public DrainingIterator<E[]> drainingIterator() {
        return new DrainingIterator<>(pages, PAGE_SIZE);
    }

    public void forAll(LongObjectConsumer<E> consumer) {
        E[][] pages = this.pages;
        for (int pageIndex = 0; pageIndex < pages.length; pageIndex++) {
            E[] page = pages[pageIndex];
            if (page == null) {
                continue;
            }
            for (int indexInPage = 0; indexInPage < page.length; indexInPage++) {
                E value = page[indexInPage];
                if (value.equals(defaultValue)) {
                    continue;
                }
                long index = ((long) pageIndex << PAGE_SHIFT) | (long) indexInPage;
                consumer.consume(index, value);
            }
        }
    }

    public void set(long index, E value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        getPage(pageIndex)[indexInPage] = value;
    }

    public Stream<E> stream() {
        return Arrays.stream(this.pages).filter(obj -> !(obj == null)).flatMap(Arrays::stream);
    }

    public boolean setIfAbsent(long index, E value) {
        int pageIndex = PageUtil.pageIndex(index, PAGE_SHIFT);
        int indexInPage = PageUtil.indexInPage(index, PAGE_MASK);
        E[] page = getPage(pageIndex);
        E currentValue = page[indexInPage];
        if (currentValue.equals(defaultValue)) {
            page[indexInPage] = value;
            return true;
        }
        return false;
    }

    private E[] getPage(int pageIndex) {
        if (pageIndex >= pages.length) {
            grow(pageIndex + 1);
        }
        E[] page = pages[pageIndex];
        if (page == null) {
            page = allocateNewPage(pageIndex);
        }
        return page;
    }

    private void grow(int minNewSize) {
        if (minNewSize <= pages.length) {
            return;
        }
        int newSize = ArrayUtil.oversize(minNewSize, Estimate.BYTES_OBJECT_REF);
        this.pages = Arrays.copyOf(this.pages, newSize);
    }

    private E[] allocateNewPage(int pageIndex) {
        E[] page = (E[]) Array.newInstance(clazz, PAGE_SIZE);
        if (defaultValue != null) {
            Arrays.fill(page, defaultValue);
        }
        this.pages[pageIndex] = page;
        return page;
    }
}
