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
package org.neo4j.graphalgo.core.utils.dss;

import org.junit.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.RankedDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.UnionStrategy;

import static org.junit.Assert.assertEquals;

public class RankedDisjointSetStructTest extends DisjointSetStructTest {

    @Override
    DisjointSetStruct newSet(final int capacity) {
        AllocationTracker tracker = AllocationTracker.EMPTY;
        return new RankedDisjointSetStruct(
                capacity,
                new UnionStrategy.ByRank(capacity, tracker),
                tracker);
    }

    @Test
    public void shouldComputeMemoryEstimation() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(
                MemoryRange.of(112),
                RankedDisjointSetStruct.memoryEstimation().estimate(dimensions0, 1).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(1712),
                RankedDisjointSetStruct.memoryEstimation().estimate(dimensions100, 1).memoryUsage());

        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
        assertEquals(MemoryRange.of(1600244140768L), RankedDisjointSetStruct
                .memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
    }
}
