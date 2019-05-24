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
package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongDoubleHashMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static com.carrotsearch.hppc.Containers.DEFAULT_EXPECTED_ELEMENTS;
import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.shallowSizeOfInstance;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfLongArray;


public final class TrackingLongDoubleHashMap extends LongDoubleHashMap {
    private final AllocationTracker tracker;

    private static final long CLASS_MEMORY = shallowSizeOfInstance(TrackingLongDoubleHashMap.class);

    public TrackingLongDoubleHashMap(AllocationTracker tracker) {
        super(DEFAULT_EXPECTED_ELEMENTS, DEFAULT_LOAD_FACTOR, HashOrderMixing.defaultStrategy());
        tracker.add(CLASS_MEMORY + buffersMemorySize(keys.length));
        this.tracker = tracker;
    }

    public synchronized void synchronizedPut(long key, double value) {
        put(key, value);
    }

    @Override
    protected void allocateBuffers(final int arraySize) {
        // also during super class init where tracker field is not yet initialized
        if (!AllocationTracker.isTracking(tracker)) {
            super.allocateBuffers(arraySize);
            return;
        }
        long sizeBefore = buffersMemorySize(keys.length);
        super.allocateBuffers(arraySize);
        tracker.add(buffersMemorySize(keys.length) - sizeBefore);
    }

    public long free() {
        if (keys != null) {
            long releasable = CLASS_MEMORY + buffersMemorySize(keys.length);

            assigned = 0;
            hasEmptyKey = false;
            keys = null;
            values = null;

            return releasable;
        }
        return 0L;
    }

    @Override
    public void release() {
        free();
    }

    private long buffersMemorySize(int length) {
        return sizeOfLongArray(length) + sizeOfDoubleArray(length);
    }
}
