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
package org.neo4j.gds.kmeans;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.mem.MemoryUsage;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterManagerTest {

    @Test
    void memoryEstimation() {
        var k = 3;
        var dimensions = 5;
        var estimation = ClusterManager.memoryEstimation(k, dimensions)
            .estimate(GraphDimensions.of(42, 1337), 4);

        var usage = estimation.memoryUsage();

        var instanceSize = MemoryUsage.sizeOfInstance(ClusterManager.class);
        var nodesInClusterSize = MemoryUsage.sizeOfLongArray(k);
        var shouldResetSize = MemoryUsage.sizeOfArray(k, 1L);

        var total = instanceSize + nodesInClusterSize + shouldResetSize;

        var floatCentroidsSize = MemoryUsage.sizeOfFloatArray(dimensions);
        var doubleCentroidsSize = MemoryUsage.sizeOfDoubleArray(dimensions);

        assertThat(usage.min)
            .as("Min should be correct")
            .isEqualTo(total + floatCentroidsSize);
        assertThat(usage.max)
            .as("Max should be correct")
            .isEqualTo(total + doubleCentroidsSize);
    }

}
