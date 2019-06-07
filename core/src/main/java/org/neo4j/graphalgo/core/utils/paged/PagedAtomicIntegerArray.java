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

import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;

public final class PagedAtomicIntegerArray extends PagedDataStructure<AtomicIntegerArray> {

    private static final PageAllocator.Factory<AtomicIntegerArray> ALLOCATOR_FACTORY;

    static {
        int pageSize = PageUtil.pageSizeFor(Integer.BYTES);
        long pageUsage = sizeOfInstance(AtomicIntegerArray.class) + sizeOfIntArray(pageSize);

        ALLOCATOR_FACTORY = PageAllocator.of(
                pageSize,
                pageUsage,
                () -> new AtomicIntegerArray(pageSize),
                new AtomicIntegerArray[0]);
    }


    public static PagedAtomicIntegerArray newArray(long size, AllocationTracker tracker) {
        return new PagedAtomicIntegerArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private PagedAtomicIntegerArray(
            final long size,
            final PageAllocator<AtomicIntegerArray> allocator) {
        super(size, allocator);
    }

    public int get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex].get(indexInPage);
    }

    public void set(long index, int value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex].set(indexInPage, value);
    }

    public void add(long index, int delta) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex].addAndGet(indexInPage, delta);
    }

    public boolean cas(long index, int expected, int update) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex].compareAndSet(indexInPage, expected, update);
    }

    public static class Translator implements PropertyTranslator.OfInt<PagedAtomicIntegerArray> {

        public static final PagedAtomicIntegerArray.Translator INSTANCE = new PagedAtomicIntegerArray.Translator();

        @Override
        public int toInt(final PagedAtomicIntegerArray data, final long nodeId) {
            return data.get((int) nodeId);
        }
    }

}
