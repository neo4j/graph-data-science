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

import org.neo4j.graphalgo.core.utils.BitUtil;

import java.lang.reflect.Array;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfArray;

public abstract class PageAllocator<T> {

    public abstract T newPage();

    public abstract int pageSize();

    public abstract T[] emptyPages();

    public abstract long bytesPerPage();

    public final long estimateMemoryUsage(long size) {
        long numPages = PageUtil.numPagesFor(size, pageSize());
        return numPages * bytesPerPage();
    }

    public static <T> Factory<T> of(
            int pageSize,
            long bytesPerPage,
            Supplier<T> newPage,
            T[] emptyPages) {
        return new Factory<>(pageSize, bytesPerPage, pageFactory(newPage, bytesPerPage), emptyPages);
    }

    public static <T> Factory<T> of(
            int pageSize,
            long bytesPerPage,
            PageFactory<T> newPage,
            T[] emptyPages) {
        return new Factory<>(pageSize, bytesPerPage, newPage, emptyPages);
    }

    @SuppressWarnings("unchecked")
    public static <T> Factory<T> ofArray(Class<T> arrayClass) {
        Class<?> componentType = arrayClass.getComponentType();
        assert componentType != null && componentType.isPrimitive();

        long bytesPerElement = sizeOfInstance(componentType);
        int pageSize = PageUtil.pageSizeFor((int) bytesPerElement);

        long bytesPerPage = sizeOfArray(pageSize, bytesPerElement);

        T[] emptyPages = (T[]) Array.newInstance(componentType, 0, 0);
        PageFactory<T> newPage = (tracker) -> {
            tracker.add(bytesPerPage);
            return (T) Array.newInstance(componentType, pageSize);
        };

        return of(pageSize, bytesPerPage, newPage, emptyPages);
    }

    @SuppressWarnings("unchecked")
    public static <T> Factory<T> ofArray(Class<T> arrayClass, int pageSize) {
        Class<?> componentType = arrayClass.getComponentType();
        assert componentType != null && componentType.isPrimitive();

        long bytesPerElement = sizeOfInstance(componentType);
        long bytesPerPage = sizeOfArray(pageSize, bytesPerElement);

        T[] emptyPages = (T[]) Array.newInstance(componentType, 0, 0);
        PageFactory<T> newPage = (tracker) -> {
            tracker.add(bytesPerPage);
            return (T) Array.newInstance(componentType, pageSize);
        };

        return of(pageSize, bytesPerPage, newPage, emptyPages);
    }

    public static final class Factory<T> {
        private final int pageSize;
        private final long bytesPerPage;
        private final PageFactory<T> newPage;

        private final T[] emptyPages;

        private Factory(
                int pageSize,
                long bytesPerPage,
                PageFactory<T> newPage,
                T[] emptyPages) {
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
            this.newPage = newPage;
            this.emptyPages = emptyPages;
        }

        public long estimateMemoryUsage(long size) {
            long numPages = PageUtil.numPagesFor(size, pageSize);
            return numPages * bytesPerPage;
        }

        public long estimateMemoryUsage(long size, Class<?> container) {
            return sizeOfInstance(container) + estimateMemoryUsage(size);
        }

        int pageSize() {
            return pageSize;
        }

        PageAllocator<T> newAllocator(AllocationTracker tracker) {
            if (AllocationTracker.isTracking(tracker)) {
                return new TrackingAllocator<>(
                        newPage,
                        emptyPages,
                        pageSize,
                        bytesPerPage,
                        tracker);
            }
            return new DirectAllocator<>(newPage, emptyPages, pageSize, bytesPerPage);
        }
    }

    @FunctionalInterface
    public interface PageFactory<T> {
        T newPage(AllocationTracker tracker);

        default T newPage() {
            return newPage(AllocationTracker.EMPTY);
        }
    }

    private static <T> PageFactory<T> pageFactory(Supplier<T> newPage, long bytesPerPage) {
        return tracker -> {
            tracker.add(bytesPerPage);
            return newPage.get();
        };
    }

    private static final class TrackingAllocator<T> extends PageAllocator<T> {

        private final PageFactory<T> newPage;
        private final T[] emptyPages;
        private final int pageSize;
        private final long bytesPerPage;
        private final AllocationTracker tracker;

        private TrackingAllocator(
                PageFactory<T> newPage,
                T[] emptyPages,
                int pageSize,
                long bytesPerPage,
                AllocationTracker tracker) {
            this.emptyPages = emptyPages;
            assert BitUtil.isPowerOfTwo(pageSize);
            this.newPage = newPage;
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
            this.tracker = tracker;
        }

        @Override
        public T newPage() {
            return newPage.newPage(tracker);
        }

        @Override
        public int pageSize() {
            return pageSize;
        }

        @Override
        public long bytesPerPage() {
            return bytesPerPage;
        }

        @Override
        public T[] emptyPages() {
            return emptyPages;
        }
    }

    private static final class DirectAllocator<T> extends PageAllocator<T> {

        private final PageFactory<T> newPage;
        private final T[] emptyPages;
        private final int pageSize;
        private final long bytesPerPage;

        private DirectAllocator(
                PageFactory<T> newPage,
                T[] emptyPages,
                int pageSize,
                long bytesPerPage) {
            assert BitUtil.isPowerOfTwo(pageSize);
            this.emptyPages = emptyPages;
            this.newPage = newPage;
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
        }

        @Override
        public T newPage() {
            return newPage.newPage();
        }

        @Override
        public int pageSize() {
            return pageSize;
        }

        @Override
        public long bytesPerPage() {
            return bytesPerPage;
        }

        @Override
        public T[] emptyPages() {
            return emptyPages;
        }
    }
}

