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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.TrackingLongDoubleHashMap;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.BitUtil;

import java.util.Arrays;
import java.util.Objects;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;


abstract class HugeWeightMap {

    static HugeWeightMapping of(Page[] pages, int pageSize, double defaultValue, AllocationTracker tracker) {
        if (pages.length == 1) {
            Page page = pages[0];
            page.setDefaultValue(defaultValue);
            return page;
        }
        return new PagedHugeWeightMap(pages, pageSize, defaultValue, tracker);
    }

    static MemoryEstimation memoryEstimation(int pageSize, int numberOfPages) {
        if (numberOfPages == 1) {
            return Page.memoryEstimation(pageSize);
        } else {
            return PagedHugeWeightMap.memoryEstimation(pageSize, numberOfPages);
        }
    }

    static MemoryEstimation memoryEstimation(String description) {
        return MemoryEstimations.setup(description, (dimensions, concurrency) -> {
            ImportSizing importSizing = ImportSizing.of(concurrency, dimensions.nodeCount());
            return HugeWeightMap.memoryEstimation(importSizing.pageSize(), importSizing.numberOfPages());
        });

    }

    private HugeWeightMap() {
    }

    static final class Page implements HugeWeightMapping {
        private static final long CLASS_MEMORY = sizeOfInstance(Page.class);

        private TrackingLongDoubleHashMap[] data;
        private final AllocationTracker tracker;
        private double defaultValue;

        static MemoryEstimation memoryEstimation(int pageSize) {
            return MemoryEstimations.builder(Page.class)
                    .add("data", TrackingLongDoubleHashMap.memoryEstimation(pageSize))
                    .build();
        }

        Page(int pageSize, AllocationTracker tracker) {
            this.data = new TrackingLongDoubleHashMap[pageSize];
            this.tracker = tracker;
            tracker.add(CLASS_MEMORY + sizeOfObjectArray(pageSize));
        }

        @Override
        public long size() {
            return Arrays
                    .stream(data)
                    .filter(Objects::nonNull)
                    .mapToLong(TrackingLongDoubleHashMap::size)
                    .sum();
        }

        @Override
        public double weight(final long source, final long target) {
            return weight(source, target, defaultValue);
        }

        @Override
        public double weight(final long source, final long target, final double defaultValue) {
            int localIndex = (int) source;
            return get(localIndex, target, defaultValue);
        }

        double get(int localIndex, long target, double defaultValue) {
            TrackingLongDoubleHashMap map = data[localIndex];
            return map != null ? map.getOrDefault(target, defaultValue) : defaultValue;
        }

        void put(int localIndex, long target, double value) {
            mapForIndex(localIndex).put(target, value);
        }

        @Override
        public long release() {
            if (data != null) {
                long released = CLASS_MEMORY + sizeOfObjectArray(data.length);
                for (TrackingLongDoubleHashMap map : data) {
                    if (map != null) {
                        released += map.free();
                    }
                }
                data = null;
                return released;
            }
            return 0L;
        }

        private void setDefaultValue(double defaultValue) {
            this.defaultValue = defaultValue;
        }

        private TrackingLongDoubleHashMap mapForIndex(int localIndex) {
            TrackingLongDoubleHashMap map = data[localIndex];
            if (map == null) {
                map = data[localIndex] = new TrackingLongDoubleHashMap(tracker);
            }
            return map;
        }
    }

    private static final class PagedHugeWeightMap implements HugeWeightMapping {

        private static final long CLASS_MEMORY = sizeOfInstance(PagedHugeWeightMap.class);

        private final int pageShift;
        private final long pageMask;

        private final double defaultValue;

        private Page[] pages;

        static MemoryEstimation memoryEstimation(int pageSize, int numberOfPages) {
            return MemoryEstimations.builder(PagedHugeWeightMap.class)
                    .fixed("pages wrapper", sizeOfObjectArray(numberOfPages))
                    .add("page[]", Page.memoryEstimation(pageSize).times(numberOfPages))
                    .build();
        }

        PagedHugeWeightMap(Page[] pages, int pageSize, double defaultValue, AllocationTracker tracker) {
            assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
            this.defaultValue = defaultValue;
            this.pages = pages;
            tracker.add(CLASS_MEMORY + sizeOfObjectArray(pages.length));
        }

        @Override
        public long size() {
            return Arrays
                    .stream(pages)
                    .filter(Objects::nonNull)
                    .mapToLong(Page::size)
                    .sum();
        }

        @Override
        public double weight(final long source, final long target) {
            return weight(source, target, defaultValue);
        }

        @Override
        public double weight(final long source, final long target, final double defaultValue) {
            int pageIndex = (int) (source >>> pageShift);
            Page page = pages[pageIndex];
            if (page != null) {
                return page.get((int) (source & pageMask), target, defaultValue);
            }
            return defaultValue;
        }

        public double defaultValue() {
            return defaultValue;
        }

        @Override
        public long release() {
            if (pages != null) {
                long released = CLASS_MEMORY + sizeOfObjectArray(pages.length);
                for (Page page : pages) {
                    if (page != null) {
                        released += page.release();
                    }
                }
                pages = null;
                return released;
            }
            return 0L;
        }
    }
}
