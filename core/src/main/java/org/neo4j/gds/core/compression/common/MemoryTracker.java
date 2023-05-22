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
package org.neo4j.gds.core.compression.common;

import org.HdrHistogram.ConcurrentHistogram;
import org.neo4j.gds.utils.GdsFeatureToggles;

public abstract class MemoryTracker {

    private final ConcurrentHistogram heapAllocations;
    private final ConcurrentHistogram nativeAllocations;
    private final ConcurrentHistogram pageSizes;
    private final ConcurrentHistogram headerBits;
    private final ConcurrentHistogram headerAllocations;

    public static MemoryTracker of() {
        return GdsFeatureToggles.ENABLE_ADJACENCY_COMPRESSION_MEMORY_TRACKING.isEnabled() ? new NonEmpty() : EMPTY;
    }

    public static MemoryTracker empty() {
        return EMPTY;
    }

    MemoryTracker() {
        this.heapAllocations = new ConcurrentHistogram(0);
        this.nativeAllocations = new ConcurrentHistogram(0);
        this.pageSizes = new ConcurrentHistogram(0);
        this.headerBits = new ConcurrentHistogram(0);
        this.headerAllocations = new ConcurrentHistogram(0);
    }

    public void recordHeapAllocation(long size) {
        this.heapAllocations.recordValue(size);
    }

    public void recordNativeAllocation(long size) {
        this.nativeAllocations.recordValue(size);
    }

    public void recordPageSize(int size) {
        this.pageSizes.recordValue(size);
    }

    public void recordHeaderBits(int bits) {
        this.headerBits.recordValue(bits);
    }

    public void recordHeaderAllocation(long size) {
        this.headerAllocations.recordValue(size);
    }

    public ImmutableHistogram heapAllocations() {
        return new ImmutableHistogram(heapAllocations);
    }

    public ImmutableHistogram nativeAllocations() {
        return new ImmutableHistogram(nativeAllocations);
    }

    public ImmutableHistogram pageSizes() {
        return new ImmutableHistogram(pageSizes);
    }

    public ImmutableHistogram headerBits() {
        return new ImmutableHistogram(headerBits);
    }

    public ImmutableHistogram headerAllocations() {
        return new ImmutableHistogram(headerAllocations);
    }

    private static final class NonEmpty extends MemoryTracker {
    }

    private static final MemoryTracker EMPTY = new MemoryTracker() {

        @Override
        public void recordHeapAllocation(long size) {}

        @Override
        public void recordNativeAllocation(long size) {}

        @Override
        public void recordPageSize(int size) {}

        @Override
        public void recordHeaderBits(int bits) {}

        @Override
        public void recordHeaderAllocation(long size) {}
    };
}
