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
package org.neo4j.gds.core.compression.varlong;

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
import static org.neo4j.gds.core.compression.varlong.CompressedAdjacencyList.computeAdjacencyByteSize;
import static org.neo4j.gds.mem.BitUtil.ceilDiv;

class CompressedAdjacencyListTest {

    @Test
    void shouldComputeCompressedMemoryEstimationForSinglePage() {
        var nodeCount = 100;
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(nodeCount)
            .relCountUpperBound(100)
            .build();

        MemoryTree memRec = CompressedAdjacencyList.adjacencyListEstimation(false).estimate(dimensions, 1);

        long classSize = 24;
        long bestCaseAdjacencySize = 500;
        long worstCaseAdjacencySize = 500;

        int minPages = PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        int maxPages = PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        long bytesPerPage = BitUtil.align(16 + 262144L, 8);
        long minAdjacencyPages = minPages * bytesPerPage + BitUtil.align(16 + minPages * 4, 8);
        long maxAdjacencyPages = maxPages * bytesPerPage + BitUtil.align(16 + maxPages * 4, 8);

        long degrees = HugeIntArray.memoryEstimation(nodeCount);
        long offsets = HugeLongArray.memoryEstimation(nodeCount);

        MemoryRange expected = MemoryRange.of(
            classSize + minAdjacencyPages + degrees + offsets,
            classSize + maxAdjacencyPages + degrees + offsets
        );

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    void shouldComputeCompressedMemoryEstimationForMultiplePage() {
        var nodeCount = 100_000_000L;
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(nodeCount)
            .relCountUpperBound(100_000_000_000L)
            .build();

        MemoryTree memRec = CompressedAdjacencyList.adjacencyListEstimation(false).estimate(dimensions, 1);

        long classSize = 24;
        long bestCaseAdjacencySize = 100_100_000_000L;
        long worstCaseAdjacencySize = 299_900_000_000L;

        int minPages = PageUtil.numPagesFor(bestCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        int maxPages = PageUtil.numPagesFor(worstCaseAdjacencySize, PAGE_SHIFT, PAGE_MASK);
        long bytesPerPage = BitUtil.align(16 + 262144L, 8);
        long minAdjacencyPages = minPages * bytesPerPage + BitUtil.align(16 + minPages * 4, 8);
        long maxAdjacencyPages = maxPages * bytesPerPage + BitUtil.align(16 + maxPages * 4, 8);

        long degrees = HugeIntArray.memoryEstimation(nodeCount);
        long offsets = HugeLongArray.memoryEstimation(nodeCount);

        MemoryRange expected = MemoryRange.of(
            classSize + minAdjacencyPages + degrees + offsets,
            classSize + maxAdjacencyPages + degrees + offsets
        );

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    void shouldComputeAdjacencyByteSize() {
        long avgDegree = 1000;
        long nodeCount = 100_000_000;
        long delta = 100_000;
        long firstAdjacencyIdAvgByteSize = ceilDiv(ceilDiv(64 - Long.numberOfLeadingZeros(nodeCount - 1), 7), 2);
        // int relationshipByteSize = encodedVLongSize(delta);
        long relationshipByteSize = ceilDiv(64 - Long.numberOfLeadingZeros(delta - 1), 7);
        long compressedAdjacencyByteSize = relationshipByteSize * (avgDegree - 1);
        long expected = (firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize) * nodeCount;

        assertEquals(expected, computeAdjacencyByteSize(avgDegree, nodeCount, delta));
    }

    @Test
    void shouldComputeAdjacencyByteSizeNoNodes() {
        long avgDegree = 0;
        long nodeCount = 0;
        long delta = 0;
        assertEquals(0, computeAdjacencyByteSize(avgDegree, nodeCount, delta));
    }

    @Test
    void shouldComputeAdjacencyByteSizeNoRelationships() {
        long avgDegree = 0;
        long nodeCount = 100;
        long delta = 0;
        assertEquals(0, computeAdjacencyByteSize(avgDegree, nodeCount, delta));
    }
}
