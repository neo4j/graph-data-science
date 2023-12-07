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
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KmeansMemoryEstimateDefinitionTest {

    @Test
    void memoryEstimation() {

        var config = mock(KmeansBaseConfig.class);
        when(config.k()).thenReturn(10);

        var graphDimensions = GraphDimensions.of(42, 1337);

        var memoryEstimation = new KmeansMemoryEstimateDefinition().memoryEstimation(config);

        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(graphDimensions, 4)
            .hasMin(33952L)
            .hasMax(54944L);

    }

    @Test
    void memoryEstimationWithSeededCentroids() {

        var centroidsSeeds = List.of(List.of(40.712776, -74.005974), List.of(52.370216, 4.895168), List.of(41.902782, 12.496365));
        var graphDimensions = GraphDimensions.of(42, 1337);

        var config = mock(KmeansBaseConfig.class);
        when(config.k()).thenReturn(10);
        when(config.seedCentroids()).thenReturn(centroidsSeeds);
        when(config.isSeeded()).thenReturn(true);
        var memoryEstimation = new KmeansMemoryEstimateDefinition().memoryEstimation(config);

        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(graphDimensions, 4)
            .hasMin(33952L + MemoryUsage.sizeOf(centroidsSeeds))
            .hasMax(54944L + MemoryUsage.sizeOf(centroidsSeeds));
    }

    @Test
    void memoryEstimationWithSilhouette() {
        var graphDimensions = GraphDimensions.of(42, 1337);

        var config = mock(KmeansBaseConfig.class);
        when(config.k()).thenReturn(10);
        when(config.computeSilhouette()).thenReturn(true);

        var memoryEstimation = new KmeansMemoryEstimateDefinition().memoryEstimation(config);

        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(graphDimensions, 4)
            .hasMin(34328L)
            .hasMax(55320L);

    }

}
