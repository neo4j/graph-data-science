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
import org.neo4j.graphalgo.core.utils.container.TrackingLongDoubleHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.BitUtil;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfObjectArray;


abstract class HugeWeightMap {

    static HugeWeightMapping of(Page[] pages, int pageSize, double defaultValue, AllocationTracker tracker) {
        if (pages.length == 1) {
            Page page = pages[0];
            page.setDefaultValue(defaultValue);
            return page;
        }
        return new PagedHugeWeightMap(pages, pageSize, defaultValue, tracker);
    }

    private HugeWeightMap() {
    }

    static final class Page implements HugeWeightMapping {
        private static final long CLASS_MEMORY = shallowSizeOfInstance(Page.class);

        private TrackingLongDoubleHashMap[] data;
        private final AllocationTracker tracker;
        private double defaultValue;

        Page(int pageSize, AllocationTracker tracker) {
            this.data = new TrackingLongDoubleHashMap[pageSize];
            this.tracker = tracker;
            tracker.add(CLASS_MEMORY + sizeOfObjectArray(pageSize));
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

        private static final long CLASS_MEMORY = shallowSizeOfInstance(PagedHugeWeightMap.class);

        private final int pageShift;
        private final long pageMask;

        private final double defaultValue;

        private Page[] pages;

        PagedHugeWeightMap(Page[] pages, int pageSize, double defaultValue, AllocationTracker tracker) {
            assert pageSize == 0 || BitUtil.isPowerOfTwo(pageSize);
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = (long) (pageSize - 1);
            this.defaultValue = defaultValue;
            this.pages = pages;
            tracker.add(CLASS_MEMORY + sizeOfObjectArray(pages.length));
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
