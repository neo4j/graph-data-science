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

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import java.util.Arrays;
import java.util.Objects;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.parallelStream;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

public final class PagedLongDoubleMap {

    private static final int PAGE_SHIFT = 14;
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final long PAGE_MASK = (long) (PAGE_SIZE - 1);

    private static final MemoryEstimation MEMORY_REQUIREMENTS = MemoryEstimations
            .builder(PagedLongDoubleMap.class)
            .add(MemoryEstimations.setup("pages[]", dimensions -> {
                int numPages = PageUtil.numPagesFor(dimensions.nodeCount(), PAGE_SHIFT, PAGE_MASK);
                long pagesArraySize = sizeOfObjectArray(numPages);
                MemoryEstimation pagesSize = MemoryEstimations.andThen(
                        TrackingIntDoubleHashMap.memoryEstimation(),
                        range -> range.times(numPages).union(MemoryRange.empty()));
                return MemoryEstimations.builder()
                        .add(pagesSize)
                        .fixed("pages wrapper", pagesArraySize)
                        .build();
            })).build();

    public static PagedLongDoubleMap of(long size, AllocationTracker tracker) {
        int numPages = PageUtil.numPagesFor(size, PAGE_SHIFT, PAGE_MASK);
        tracker.add(sizeOfObjectArray(numPages));
        TrackingIntDoubleHashMap[] pages = new TrackingIntDoubleHashMap[numPages];
        return new PagedLongDoubleMap(pages, tracker);
    }

    private final AllocationTracker tracker;
    private TrackingIntDoubleHashMap[] pages;

    public static MemoryEstimation memoryEstimation() {
        return MEMORY_REQUIREMENTS;
    }

    private PagedLongDoubleMap(
            TrackingIntDoubleHashMap[] pages,
            AllocationTracker tracker) {
        this.pages = pages;
        this.tracker = tracker;
    }

    public long size() {
        return parallelStream(Arrays.stream(pages), stream -> stream
                .filter(Objects::nonNull)
                .mapToLong(TrackingIntDoubleHashMap::size)
                .sum());
    }

    public double getOrDefault(long index, double defaultValue) {
        int pageIndex = pageIndex(index);
        if (pageIndex < pages.length) {
            IntDoubleMap page = pages[pageIndex];
            if (page != null) {
                int indexInPage = indexInPage(index);
                return page.getOrDefault(indexInPage, defaultValue);
            }
        }
        return defaultValue;
    }

    public void put(long index, double value) {
        int pageIndex = pageIndex(index);
        TrackingIntDoubleHashMap subMap = subMap(pageIndex);
        int indexInPage = indexInPage(index);
        subMap.putSync(indexInPage, value);
    }

    private TrackingIntDoubleHashMap subMap(int pageIndex) {
        if (pageIndex >= pages.length) {
            return growNewSubMap(pageIndex);
        }
        TrackingIntDoubleHashMap subMap = pages[pageIndex];
        if (subMap != null) {
            return subMap;
        }
        return forceNewSubMap(pageIndex);
    }

    private synchronized TrackingIntDoubleHashMap growNewSubMap(int pageIndex) {
        if (pageIndex >= pages.length) {
            long allocated = sizeOfObjectArray(1 + pageIndex) - sizeOfObjectArray(pages.length);
            tracker.add(allocated);
            pages = Arrays.copyOf(pages, 1 + pageIndex);
        }
        return forceNewSubMap(pageIndex);
    }

    private synchronized TrackingIntDoubleHashMap forceNewSubMap(int pageIndex) {
        TrackingIntDoubleHashMap subMap = pages[pageIndex];
        if (subMap == null) {
            subMap = new TrackingIntDoubleHashMap(tracker);
            pages[pageIndex] = subMap;
        }
        return subMap;
    }

    public long getMaxValue() {
        return parallelStream(Arrays.stream(pages), stream -> stream
                .filter(Objects::nonNull)
                .flatMapToDouble(TrackingIntDoubleHashMap::getValuesAsStream)
                .max()
                .orElse(0d)).longValue();
    }

    public long release() {
        if (pages != null) {
            TrackingIntDoubleHashMap[] pages = this.pages;
            this.pages = null;
            long released = sizeOfObjectArray(pages.length);
            for (TrackingIntDoubleHashMap page : pages) {
                if (page != null) {
                    released += page.instanceSize();
                }
            }
            tracker.remove(released);
            return released;
        }
        return 0L;
    }

    private static int pageIndex(long index) {
        return (int) (index >>> PAGE_SHIFT);
    }

    private static int indexInPage(long index) {
        return (int) (index & PAGE_MASK);
    }
}
