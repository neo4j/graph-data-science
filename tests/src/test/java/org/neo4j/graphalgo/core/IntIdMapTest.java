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
package org.neo4j.graphalgo.core;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class IntIdMapTest {

    @Test
    void shouldComputeMemoryEstimation() {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(100).build();
        MemoryTree memRec = IntIdMap.memoryEstimation().estimate(dimensions, 1);

        assertEquals(memRec.memoryUsage().min, 4048L);
        assertEquals(memRec.memoryUsage().max, 4048L);
    }
}
