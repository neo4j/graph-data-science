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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class KmeansTaskTest {

    @Test
    void memoryEstimation() {
        var k = 10;
        var dimensions = 128;

        var estimation = KmeansTask.memoryEstimation(k, dimensions)
            .estimate(GraphDimensions.of(42, 1337), 4);

        var usage = estimation.memoryUsage();

        var instanceSize = MemoryUsage.sizeOfInstance(KmeansTask.class);
        var communitySizesSize = MemoryUsage.sizeOfLongArray(k);
        var communityCoordinateSumsMin = k * MemoryUsage.sizeOfFloatArray(dimensions);
        var communityCoordinateSumsMax = k * MemoryUsage.sizeOfDoubleArray(dimensions);

        assertThat(usage.min).isEqualTo(instanceSize + communitySizesSize + communityCoordinateSumsMin);
        assertThat(usage.max).isEqualTo(instanceSize + communitySizesSize + communityCoordinateSumsMax);
    }
}
