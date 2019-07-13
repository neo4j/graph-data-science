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
import org.neo4j.graphalgo.core.utils.paged.dss.IncrementalDisjointSetStruct;
import org.neo4j.graphalgo.core.utils.paged.dss.UnionStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class IncrementalDisjointSetStructTest extends DisjointSetStructTest {

    @Override
    DisjointSetStruct newSet(final int capacity) {
        TestWeightMapping communities = new TestWeightMapping();
        return newSet(capacity, communities);
    }

    DisjointSetStruct newSet(final int capacity, final TestWeightMapping weightMapping) {
        return new IncrementalDisjointSetStruct(
                capacity,
                weightMapping,
                new UnionStrategy.ByMin(),
                AllocationTracker.EMPTY);
    }

    @Test
    public void shouldRunWithLessInitialCommunities() {
        TestWeightMapping communities = new TestWeightMapping(0, 0, 1, 0);
        DisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, dss.getSetCount());
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    public void shouldRunWithLessInitialCommunitiesAndLargerIdSpace() {
        TestWeightMapping communities = new TestWeightMapping(0, 10, 1, 10);
        DisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, dss.getSetCount());
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    public void shouldRunWithLessInitialCommunitiesAndOverlappingIdSpace() {
        TestWeightMapping communities = new TestWeightMapping(0, 3, 1, 3);
        DisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, dss.getSetCount());
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    public void shouldComputeMemoryEstimation() {
        GraphDimensions dimensions0 = new GraphDimensions.Builder().setNodeCount(0).build();
        assertEquals(
                MemoryRange.of(200, 264),
                IncrementalDisjointSetStruct.memoryEstimation().estimate(dimensions0, 1).memoryUsage());

        GraphDimensions dimensions100 = new GraphDimensions.Builder().setNodeCount(100).build();
        assertEquals(
                MemoryRange.of(1064, 5032),
                IncrementalDisjointSetStruct.memoryEstimation().estimate(dimensions100, 1).memoryUsage());

        // TODO: >int not supported yet
//        GraphDimensions dimensions100B = new GraphDimensions.Builder().setNodeCount(100_000_000_000L).build();
//        assertEquals(MemoryRange.of(1600244140776L), IncrementalDisjointSetStruct
//                .memoryEstimation().estimate(dimensions100B, 1).memoryUsage());
    }
}
