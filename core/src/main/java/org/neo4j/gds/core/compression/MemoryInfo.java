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
import org.neo4j.gds.core.compression.common.ImmutableHistogram;

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
     * Tracks the number of blocks in the adjacency list.
     */
    OptionalLong blockCount();

    /**
     * Tracks the standard deviation of the number of bits used to encode a block of target ids.
     */
    Optional<ImmutableHistogram> stdDevBits();

    /**
     * Tracks the mean of the number of bits used to encode a block of target ids.
     */
    Optional<ImmutableHistogram> meanBits();

    /**
     * Tracks the median of the number of bits used to encode a block of target ids.
     */
    Optional<ImmutableHistogram> medianBits();

    /**
     * Tracks the block lengths.
     */
    Optional<ImmutableHistogram> blockLengths();

    /**
     * Tracks the maximum number of bits used to encode a block of target ids.
     */
    Optional<ImmutableHistogram> maxBits();

    /**
     * Tracks the minimum number of bits used to encode a block of target ids.
     */
    Optional<ImmutableHistogram> minBits();

    /**
     * Tracks the index of the min value within a block of target ids.
     */
    Optional<ImmutableHistogram> indexOfMinValue();

    /**
     * Tracks the index of the max value within a block of target ids.
     */
    Optional<ImmutableHistogram> indexOfMaxValue();

    /**
     * Tracks the absolute difference between the number of bits required
     * to encode the head value and the average number of bits required for the tail values.
     */
    Optional<ImmutableHistogram> headTailDiffBits();

    /**
     * Tracks the difference between the lowest and highest number of bits to encode any value in a block.
     */
    Optional<ImmutableHistogram> bestMaxDiffBits();

    /**
     * Tracks the number of exceptions within a block according to the PFOR heuristic.
     */
    Optional<ImmutableHistogram> pforExceptions();

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
            .blockCount(LongStream.concat(blockCount().stream(), other.blockCount().stream()).reduce(Long::sum))
            .blockLengths(blockLengths()
                .map(left -> other.blockLengths().map(left::merge).orElse(left))
                .or(other::blockLengths))
            .stdDevBits(stdDevBits()
                .map(left -> other.stdDevBits().map(left::merge).orElse(left))
                .or(other::stdDevBits))
            .meanBits(meanBits()
                .map(left -> other.meanBits().map(left::merge).orElse(left))
                .or(other::meanBits))
            .medianBits(medianBits()
                .map(left -> other.medianBits().map(left::merge).orElse(left))
                .or(other::medianBits))
            .blockLengths(blockLengths()
                .map(left -> other.blockLengths().map(left::merge).orElse(left))
                .or(other::blockLengths))
            .maxBits(maxBits()
                .map(left -> other.maxBits().map(left::merge).orElse(left))
                .or(other::maxBits))
            .minBits(minBits()
                .map(left -> other.minBits().map(left::merge).orElse(left))
                .or(other::minBits))
            .indexOfMaxValue(indexOfMaxValue()
                .map(left -> other.indexOfMaxValue().map(left::merge).orElse(left))
                .or(other::indexOfMaxValue))
            .indexOfMinValue(indexOfMinValue()
                .map(left -> other.indexOfMinValue().map(left::merge).orElse(left))
                .or(other::indexOfMinValue))
            .headTailDiffBits(headTailDiffBits()
                .map(left -> other.headTailDiffBits().map(left::merge).orElse(left))
                .or(other::headTailDiffBits))
            .bestMaxDiffBits(bestMaxDiffBits()
                .map(left -> other.bestMaxDiffBits().map(left::merge).orElse(left))
                .or(other::bestMaxDiffBits))
            .pforExceptions(pforExceptions()
                .map(left -> other.pforExceptions().map(left::merge).orElse(left))
                .or(other::pforExceptions))
            .build();
    }
}
