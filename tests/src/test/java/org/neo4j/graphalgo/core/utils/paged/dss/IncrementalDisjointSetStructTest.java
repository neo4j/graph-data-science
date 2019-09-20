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
package org.neo4j.graphalgo.core.utils.paged.dss;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphalgo.core.utils.paged.dss.IncrementalDisjointSetStruct.memoryEstimation;

class IncrementalDisjointSetStructTest extends DisjointSetStructTest {

    @Override
    SequentialDisjointSetStruct newSet(final int capacity) {
        TestWeightMapping communities = new TestWeightMapping();
        return newSet(capacity, communities);
    }

    SequentialDisjointSetStruct newSet(final int capacity, final TestWeightMapping weightMapping) {
        return new IncrementalDisjointSetStruct(
                capacity,
                weightMapping,
                AllocationTracker.EMPTY);
    }

    @Test
    void shouldRunWithLessInitialCommunities() {
        TestWeightMapping communities = new TestWeightMapping(0, 0, 1, 0);
        SequentialDisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, getSetCount(dss));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    void shouldRunWithLessInitialCommunitiesAndLargerIdSpace() {
        TestWeightMapping communities = new TestWeightMapping(0, 10, 1, 10);
        SequentialDisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, getSetCount(dss));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    void shouldRunWithLessInitialCommunitiesAndOverlappingIdSpace() {
        TestWeightMapping communities = new TestWeightMapping(0, 3, 1, 3);
        SequentialDisjointSetStruct dss = newSet(4, communities);

        assertEquals(3, getSetCount(dss));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertEquals(dss.setIdOf(0), dss.setIdOf(1));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(2));
        assertNotEquals(dss.setIdOf(0), dss.setIdOf(3));
        assertNotEquals(dss.setIdOf(2), dss.setIdOf(3));
    }

    @Test
    void shouldComputeMemoryEstimation() {
        assertMemoryEstimation(memoryEstimation(), 0, MemoryRange.of(296));
        assertMemoryEstimation(memoryEstimation(), 100, MemoryRange.of(2696));
        assertMemoryEstimation(memoryEstimation(), 100_000_000_000L, MemoryRange.of(2_400_366_211_280L));
    }
}
