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

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.IntDoubleHashMap;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.concurrent.atomic.LongAdder;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;

import static com.carrotsearch.hppc.Containers.DEFAULT_EXPECTED_ELEMENTS;
import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;


final class TrackingIntDoubleHashMap extends IntDoubleHashMap {

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations
            .builder(TrackingIntDoubleHashMap.class)
            .rangePerNode("map buffers", nodeCount -> {
                long minBufferSize = MemoryUsage.sizeOfEmptyHashContainer();
                long maxBufferSize = MemoryUsage.sizeOfHashContainer(Math.min(PagedLongDoubleMap.PAGE_SIZE, nodeCount));
                long min = sizeOfIntArray(minBufferSize) + sizeOfDoubleArray(minBufferSize);
                long max = sizeOfIntArray(maxBufferSize) + sizeOfDoubleArray(maxBufferSize);
                return MemoryRange.of(min, max);
            })
            .build();

    private final AllocationTracker tracker;
    private final LongAdder instanceSize;

    static MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    public TrackingIntDoubleHashMap(AllocationTracker tracker) {
        super(DEFAULT_EXPECTED_ELEMENTS, DEFAULT_LOAD_FACTOR, HashOrderMixing.defaultStrategy());
        this.tracker = tracker;
        this.instanceSize = new LongAdder();
        trackUsage(bufferUsage(keys.length));
    }

    @Override
    protected void allocateBuffers(final int arraySize) {
        // also during super class init where tracker is not yet initialized
        if (!AllocationTracker.isTracking(tracker)) {
            super.allocateBuffers(arraySize);
            return;
        }
        int lengthBefore = keys.length;
        super.allocateBuffers(arraySize);
        int lengthAfter = keys.length;
        long addedMemory = bufferUsage(lengthAfter) - bufferUsage(lengthBefore);
        trackUsage(addedMemory);
    }

    public DoubleStream getValuesAsStream() {
        return StreamSupport
                .stream(values().spliterator(), false)
                .mapToDouble(c -> c.value);
    }

    private long bufferUsage(int length) {
        return sizeOfIntArray(length) + sizeOfDoubleArray(length);
    }

    private void trackUsage(long addedMemory) {
        tracker.add(addedMemory);
        instanceSize.add(addedMemory);
    }

    public long instanceSize() {
        return instanceSize.sum();
    }

    public synchronized void putSync(int key, double value) {
        put(key, value);
    }
}
