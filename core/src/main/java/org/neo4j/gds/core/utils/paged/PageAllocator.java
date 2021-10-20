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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.MemoryUsage;

import java.lang.reflect.Array;
import java.util.function.Supplier;

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

        long bytesPerElement = MemoryUsage.sizeOfInstance(componentType);
        int pageSize = PageUtil.pageSizeFor((int) bytesPerElement);

        long bytesPerPage = MemoryUsage.sizeOfArray(pageSize, bytesPerElement);

        T[] emptyPages = (T[]) Array.newInstance(componentType, 0, 0);
        PageFactory<T> newPage = (allocationTracker) -> {
            allocationTracker.add(bytesPerPage);
            return (T) Array.newInstance(componentType, pageSize);
        };

        return of(pageSize, bytesPerPage, newPage, emptyPages);
    }

    @SuppressWarnings("unchecked")
    public static <T> Factory<T> ofArray(Class<T> arrayClass, int pageSize) {
        Class<?> componentType = arrayClass.getComponentType();
        assert componentType != null && componentType.isPrimitive();

        long bytesPerElement = MemoryUsage.sizeOfInstance(componentType);
        long bytesPerPage = MemoryUsage.sizeOfArray(pageSize, bytesPerElement);

        T[] emptyPages = (T[]) Array.newInstance(componentType, 0, 0);
        PageFactory<T> newPage = (allocationTracker) -> {
            allocationTracker.add(bytesPerPage);
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

        PageAllocator<T> newAllocator(AllocationTracker allocationTracker) {
            if (AllocationTracker.isTracking(allocationTracker)) {
                return new TrackingAllocator<>(
                        newPage,
                        emptyPages,
                        pageSize,
                        bytesPerPage,
                        allocationTracker);
            }
            return new DirectAllocator<>(newPage, emptyPages, pageSize, bytesPerPage);
        }
    }

    @FunctionalInterface
    public interface PageFactory<T> {
        T newPage(AllocationTracker allocationTracker);

        default T newPage() {
            return newPage(AllocationTracker.empty());
        }
    }

    private static <T> PageFactory<T> pageFactory(Supplier<T> newPage, long bytesPerPage) {
        return allocationTracker -> {
            allocationTracker.add(bytesPerPage);
            return newPage.get();
        };
    }

    private static final class TrackingAllocator<T> extends PageAllocator<T> {

        private final PageFactory<T> newPage;
        private final T[] emptyPages;
        private final int pageSize;
        private final long bytesPerPage;
        private final AllocationTracker allocationTracker;

        private TrackingAllocator(
                PageFactory<T> newPage,
                T[] emptyPages,
                int pageSize,
                long bytesPerPage,
                AllocationTracker allocationTracker
        ) {
            this.emptyPages = emptyPages;
            assert BitUtil.isPowerOfTwo(pageSize);
            this.newPage = newPage;
            this.pageSize = pageSize;
            this.bytesPerPage = bytesPerPage;
            this.allocationTracker = allocationTracker;
        }

        @Override
        public T newPage() {
            return newPage.newPage(allocationTracker);
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
