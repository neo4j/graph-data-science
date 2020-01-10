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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IdMapTest {

    @Test
    void shouldComputeMemoryEstimation() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(0).highestNeoId(0).build();
        MemoryTree memRec = IdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(32L + 40L + 40L), memRec.memoryUsage());

        dimensions = ImmutableGraphDimensions.builder().nodeCount(100L).highestNeoId(100L).build();
        memRec = IdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(32L + 840L + 32832L), memRec.memoryUsage());

        dimensions = ImmutableGraphDimensions.builder().nodeCount(1L).highestNeoId(100_000_000_000L).build();
        memRec = IdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(32L + 48L + 97_689_080L), memRec.memoryUsage());

        dimensions = ImmutableGraphDimensions.builder().nodeCount(10_000_000L).highestNeoId(100_000_000_000L).build();
        memRec = IdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(32L + 80_000_040L + 177_714_824L, 32L + 80_000_040L + 327_937_656_296L), memRec.memoryUsage());

        dimensions = ImmutableGraphDimensions.builder().nodeCount(100_000_000L).highestNeoId(100_000_000_000L).build();
        memRec = IdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(32L + 800_000_040L + 898_077_656L, 32L + 800_000_040L + 800_488_297_688L), memRec.memoryUsage());
    }
}
