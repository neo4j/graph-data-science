/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdjacencyOffsetsTest {

    @Test
    void shouldComputeMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryTree memRec = AdjacencyOffsets
                .memoryEstimation(4096, 1)
                .estimate(dimensions, 1);
        MemoryRange expected = MemoryRange.of(16L /* Page.class */ + BitUtil.align(16 + 4096 * 8, 8) /* data */);

        assertEquals(expected, memRec.memoryUsage());
    }

    @Test
    void shouldComputeMemoryEstimationForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        int numberOfPages = (int) BitUtil.ceilDiv(100_000, 4096);
        MemoryTree memRec = AdjacencyOffsets
                .memoryEstimation(4096, numberOfPages)
                .estimate(dimensions, 1);

        MemoryRange expected = MemoryRange.of(
                32L /* PagedOffsets.class */ +
                BitUtil.align(16 + numberOfPages * 4, 8) + /* sizeOfObjectArray(numberOfPages) */
                (BitUtil.align(16 + 4096 * 8, 8) * numberOfPages)) /* sizeOfLongArray(pageSize) * numberOfPages */;

        assertEquals(expected, memRec.memoryUsage());
    }
}
