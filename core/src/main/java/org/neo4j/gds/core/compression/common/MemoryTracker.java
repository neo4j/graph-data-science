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
import org.neo4j.gds.utils.AutoCloseableThreadLocal;
import org.neo4j.gds.utils.GdsFeatureToggles;

public interface MemoryTracker {

    MemoryTracker EMPTY = new MemoryTracker.Empty();

    static MemoryTracker create() {
        return GdsFeatureToggles.ENABLE_ADJACENCY_COMPRESSION_MEMORY_TRACKING.isEnabled()
            ? new NonEmpty()
            : empty();
    }

    static MemoryTracker empty() {
        return EMPTY;
    }

    void recordHeapAllocation(long size);

    void recordNativeAllocation(long size);

    void recordPageSize(int size);

    void recordHeaderBits(int bits);

    void recordHeaderAllocation(long size);

    void recordBlockStatistics(long[] values, int start, int length);

    ImmutableHistogram heapAllocations();

    ImmutableHistogram nativeAllocations();

    ImmutableHistogram pageSizes();

    ImmutableHistogram headerBits();

    ImmutableHistogram headerAllocations();

    BlockStatistics blockStatistics();

    class NonEmpty implements MemoryTracker {
        private final ConcurrentHistogram heapAllocations;
        private final ConcurrentHistogram nativeAllocations;
        private final ConcurrentHistogram pageSizes;
        private final ConcurrentHistogram headerBits;
        private final ConcurrentHistogram headerAllocations;
        private final AutoCloseableThreadLocal<BlockStatistics> blockStatistics;

        NonEmpty() {
            this.heapAllocations = new ConcurrentHistogram(0);
            this.nativeAllocations = new ConcurrentHistogram(0);
            this.pageSizes = new ConcurrentHistogram(0);
            this.headerBits = new ConcurrentHistogram(0);
            this.headerAllocations = new ConcurrentHistogram(0);
            this.blockStatistics = AutoCloseableThreadLocal.withInitial(BlockStatistics::new);
        }

        @Override
        public void recordHeapAllocation(long size) {
            this.heapAllocations.recordValue(size);
        }

        @Override
        public void recordNativeAllocation(long size) {
            this.nativeAllocations.recordValue(size);
        }

        @Override
        public void recordPageSize(int size) {
            this.pageSizes.recordValue(size);
        }

        @Override
        public void recordHeaderBits(int bits) {
            this.headerBits.recordValue(bits);
        }

        @Override
        public void recordHeaderAllocation(long size) {
            this.headerAllocations.recordValue(size);
        }

        @Override
        public void recordBlockStatistics(long[] values, int start, int length) {
            blockStatistics.get().record(values, start, length);
        }

        @Override
        public ImmutableHistogram heapAllocations() {
            return ImmutableHistogram.of(heapAllocations);
        }

        @Override
        public ImmutableHistogram nativeAllocations() {
            return ImmutableHistogram.of(nativeAllocations);
        }

        @Override
        public ImmutableHistogram pageSizes() {
            return ImmutableHistogram.of(pageSizes);
        }

        @Override
        public ImmutableHistogram headerBits() {
            return ImmutableHistogram.of(headerBits);
        }

        @Override
        public ImmutableHistogram headerAllocations() {
            return ImmutableHistogram.of(headerAllocations);
        }

        @Override
        public BlockStatistics blockStatistics() {
            var union = new BlockStatistics();
            blockStatistics.forEach(localStats -> localStats.mergeInto(union));
            return union;
        }
    }

    final class Empty implements MemoryTracker {

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

        @Override
        public void recordBlockStatistics(long[] values, int start, int length) {}

        @Override
        public ImmutableHistogram heapAllocations() {
            return ImmutableHistogram.EMPTY;
        }

        @Override
        public ImmutableHistogram nativeAllocations() {
            return ImmutableHistogram.EMPTY;
        }

        @Override
        public ImmutableHistogram pageSizes() {
            return ImmutableHistogram.EMPTY;
        }

        @Override
        public ImmutableHistogram headerBits() {
            return ImmutableHistogram.EMPTY;
        }

        @Override
        public ImmutableHistogram headerAllocations() {
            return ImmutableHistogram.EMPTY;
        }

        @Override
        public BlockStatistics blockStatistics() {
            return BlockStatistics.EMPTY;
        }
    }
}
