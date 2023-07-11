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
package org.neo4j.gds.api;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.compression.common.BlockStatistics;
import org.neo4j.gds.core.compression.common.ImmutableHistogram;
import org.neo4j.gds.core.compression.common.MemoryTracker;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

/**
 * The adjacency list for a mono-partite graph with an optional single relationship property.
 * Provides access to the {@link #degree(long) degree}, and the {@link #adjacencyCursor(long) target ids}.
 * The methods in here are not final and may be revised under the continuation of
 * Adjacency Compression III – Return of the Iterator
 * One particular change could be that properties will be returned from {@link AdjacencyCursor}s
 * instead from separate {@link PropertyCursor}s.
 */
public interface AdjacencyList {

    /**
     * Returns the degree of a node.
     *
     * Undefined behavior if the node does not exist.
     */
    int degree(long node);

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * The cursor is not expected to return correct property values.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(long node) {
        return adjacencyCursor(node, Double.NaN);
    }

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * If the cursor cannot produce property values, it will yield the provided {@code fallbackValue}.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * Undefined behavior if the node does not exist.
     */
    AdjacencyCursor adjacencyCursor(long node, double fallbackValue);

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * The cursor is not expected to return correct property values.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * The implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node) {
        return adjacencyCursor(reuse, node, Double.NaN);
    }

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * If the cursor cannot produce property values, it will yield the provided {@code fallbackValue}.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * The implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        return adjacencyCursor(node, fallbackValue);
    }

    /**
     * Create a new uninitialized cursor.
     *
     * NOTE: In order to use the returned cursor {@link AdjacencyCursor#init} must be called.
     */
    AdjacencyCursor rawAdjacencyCursor();

    /**
     * Returns information about on heap and off heap memory usage of this adjacency list.
     */
    MemoryInfo memoryInfo();

    AdjacencyList EMPTY = new AdjacencyList() {
        @Override
        public int degree(long node) {
            return 0;
        }

        @Override
        public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
            return AdjacencyCursor.empty();
        }

        @Override
        public AdjacencyCursor rawAdjacencyCursor() {
            return AdjacencyCursor.empty();
        }

        @Override
        public MemoryInfo memoryInfo() {
            return MemoryInfo.EMPTY;
        }

    };

    @ValueClass
    interface MemoryInfo {

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
    }
}
