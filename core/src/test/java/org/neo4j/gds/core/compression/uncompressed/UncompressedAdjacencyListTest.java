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
package org.neo4j.gds.core.compression.uncompressed;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.BitUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.core.compression.common.BumpAllocator.PAGE_MASK;
import static org.neo4j.gds.core.compression.common.BumpAllocator.PAGE_SHIFT;

class UncompressedAdjacencyListTest {

    @Test
    void shouldComputeUncompressedMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100)
            .relCountUpperBound(100)
            .build();

        MemoryTree memRec = UncompressedAdjacencyList.adjacencyListEstimation(false).estimate(dimensions, 1);

        long classSize = 24;
        long uncompressedAdjacencySize = 1200;

        int pages = PageUtil.numPagesFor(uncompressedAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        long bytesPerPage = BitUtil.align(16 + 262144L, 8);
        long adjacencyPages = pages * bytesPerPage + BitUtil.align(16 + pages * 4, 8);

        long offsets = HugeLongArray.memoryEstimation(100);
        long degrees = HugeIntArray.memoryEstimation(100);

        MemoryRange expected = MemoryRange.of(classSize + adjacencyPages + offsets + degrees);

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    void shouldComputeUncompressedMemoryEstimationForMultiplePage() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .relCountUpperBound(100_000_000_000L)
            .build();

        MemoryTree memRec = UncompressedAdjacencyList.adjacencyListEstimation(false).estimate(dimensions, 1);

        long classSize = 24;

        long uncompressedAdjacencySize = 800_000_000_000L;

        int pages = PageUtil.numPagesFor(uncompressedAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        long bytesPerPage = BitUtil.align(16 + 262144L, 8);
        long adjacencyPages = pages * bytesPerPage + BitUtil.align(16 + pages * 4, 8);

        long offsets = HugeLongArray.memoryEstimation(100_000_000L);
        long degrees = HugeIntArray.memoryEstimation(100_000_000L);

        MemoryRange expected = MemoryRange.of(classSize + adjacencyPages + offsets + degrees);

        assertEquals(expected, memRec.memoryUsage());
    }
}
