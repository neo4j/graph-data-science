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
package org.neo4j.gds.core.compression;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.compression.common.BlockStatistics;
import org.neo4j.gds.core.compression.common.ImmutableHistogram;
import org.neo4j.gds.core.compression.common.MemoryTracker;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@ValueClass
public interface MemoryInfo {

    MemoryInfo EMPTY = ImmutableMemoryInfo.builder()
        .pages(0)
        .heapAllocations(ImmutableHistogram.EMPTY)
        .nativeAllocations(ImmutableHistogram.EMPTY)
        .pageSizes(ImmutableHistogram.EMPTY)
        .headerBits(ImmutableHistogram.EMPTY)
        .headerAllocations(ImmutableHistogram.EMPTY)
        .build();

    static ImmutableMemoryInfo.Builder builder(MemoryTracker memoryTracker) {
        return ImmutableMemoryInfo.builder()
            .heapAllocations(memoryTracker.heapAllocations())
            .nativeAllocations(memoryTracker.nativeAllocations())
            .pageSizes(memoryTracker.pageSizes())
            .headerBits(memoryTracker.headerBits())
            .headerAllocations(memoryTracker.headerAllocations());
    }

    /**
     * Returns the total number of bytes occupied by this adjacency list,
     * including both, on heap and off heap.
     */
    default OptionalLong bytesTotal() {
        return Stream
            .of(bytesOnHeap(), bytesOffHeap())
            .filter(OptionalLong::isPresent)
            .mapToLong(OptionalLong::getAsLong)
            .reduce(Long::sum);
    }

    /**
     * The number of pages this adjacency list occupies.
     */
    long pages();

    /**
     * Number of bytes this adjacency list occupies on heap.
     *
     * @return Number of bytes or empty if not accessible.
     */
    OptionalLong bytesOnHeap();

    /**
     * Number of bytes this adjacency list occupies off heap.
     *
     * @return Number of bytes or empty if not accessible.
     */
    OptionalLong bytesOffHeap();

    /**
     * Histogram that tracks heap allocations sizes during adjacency list construction.
     * Each allocation is the number of bytes allocated for a single adjacency list.
     */
    ImmutableHistogram heapAllocations();

    /**
     * Histogram that tracks native allocations sizes during adjacency list construction.
     * Each allocation is the number of bytes allocated for a single adjacency list.
     */
    ImmutableHistogram nativeAllocations();

    /**
     * Histogram that tracks pages sizes of an adjacency list.
     */
    ImmutableHistogram pageSizes();

    /**
     * Histogram that tracks the number of bits used to encode a block of target ids.
     */
    ImmutableHistogram headerBits();

    /**
     * Histogram that tracks the number of bytes used to store header information for
     * a single adjacency list. That allocation is included in either {@link #heapAllocations()} or {@link #nativeAllocations()}.
     */
    ImmutableHistogram headerAllocations();

    /**
     * A collection of histograms that record various statistics for block packing.
     */
    Optional<BlockStatistics> blockStatistics();

    default MemoryInfo merge(MemoryInfo other) {
        return ImmutableMemoryInfo.builder()
            .pages(pages() + other.pages())
            .bytesOnHeap(LongStream.concat(bytesOnHeap().stream(), other.bytesOnHeap().stream()).reduce(Long::sum))
            .bytesOffHeap(LongStream.concat(bytesOffHeap().stream(), other.bytesOffHeap().stream())
                .reduce(Long::sum))
            .heapAllocations(heapAllocations().merge(other.heapAllocations()))
            .nativeAllocations(nativeAllocations().merge(other.nativeAllocations()))
            .pageSizes(pageSizes().merge(other.pageSizes()))
            .headerBits(headerBits().merge(other.headerBits()))
            .headerAllocations(headerAllocations().merge(other.headerAllocations()))
            .blockStatistics(blockStatistics().map(left -> other.blockStatistics().map(right -> {
                    var union = new BlockStatistics();
                    left.mergeInto(union);
                    right.mergeInto(union);
                    return union;
                }).orElse(left)).or(other::blockStatistics)
            ).build();
    }
}
