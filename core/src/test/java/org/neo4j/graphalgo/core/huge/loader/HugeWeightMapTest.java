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
package org.neo4j.graphalgo.core.huge.loader;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.core.utils.BitUtil.align;

public class HugeWeightMapTest {

    /**
     * Uses {@link org.neo4j.graphalgo.core.huge.loader.HugeWeightMap.Page}
     **/
    @Test
    public void shouldComputeMemoryEstimationForSinglePage() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryTree memRec = HugeWeightMap.memoryEstimation(4096, 1).apply(dimensions, 1);

        MemoryRange expected = MemoryRange.of(
                32L /* Page.class */ + 232L /* data */,
                32L /* Page.class */ + 4200 /* data */);

        assertEquals(expected, memRec.memoryUsage());
    }

    /**
     * Uses {@link org.neo4j.graphalgo.core.huge.loader.HugeWeightMap.PagedHugeWeightMap}
     **/
    @Test
    public void shouldComputeMemoryEstimationForMultiplePages() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100_000).build();
        int numberOfPages = (int) BitUtil.ceilDiv(100_000, 4096);
        MemoryTree memRec = HugeWeightMap.memoryEstimation(4096, numberOfPages).apply(dimensions, 1);

        long min =
                40 /* PagedHugeWeightMap.class */ +
                align(16 + numberOfPages * 4, 8) /* sizeOfObjectArray(numberOfPages) */ +
                numberOfPages * (32 + 232) /* Page.memoryEstimation(pageSize).times(numberOfPages)) */;

        long max =
                40 /* PagedHugeWeightMap.class */ +
                align(16 + numberOfPages * 4, 8) /* sizeOfObjectArray(numberOfPages) */ +
                numberOfPages * (32 + 131176) /* Page.memoryEstimation(pageSize).times(numberOfPages)) */;

        assertEquals(MemoryRange.of(min, max), memRec.memoryUsage());
    }
}
