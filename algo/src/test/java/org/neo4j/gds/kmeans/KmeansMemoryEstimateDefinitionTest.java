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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.Estimate;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KmeansMemoryEstimateDefinitionTest {

    @Test
    void memoryEstimation() {

        var params = mock(KmeansParameters.class);
        when(params.k()).thenReturn(10);

        var graphDimensions = GraphDimensions.of(42, 1337);

        var memoryEstimation = new KmeansMemoryEstimateDefinition(params).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(graphDimensions, new Concurrency(4))
            .hasMin(33928)
            .hasMax(54920);

    }

    @Test
    void memoryEstimationWithSeededCentroids() {

        var centroidsSeeds = List.of(List.of(40.712776, -74.005974), List.of(52.370216, 4.895168), List.of(41.902782, 12.496365));
        var graphDimensions = GraphDimensions.of(42, 1337);

        var params = mock(KmeansParameters.class);
        when(params.k()).thenReturn(10);
        when(params.seedCentroids()).thenReturn(centroidsSeeds);
        when(params.isSeeded()).thenReturn(true);
        var memoryEstimation = new KmeansMemoryEstimateDefinition(params).memoryEstimation();

        var sizeOfCentroids = 4 * Estimate.sizeOfInstance(List.class) // four lists
                            + 6 * 24; // eight doubles
        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(graphDimensions, new Concurrency(4))
            .hasMin(33928L + sizeOfCentroids)
            .hasMax(54920L + sizeOfCentroids);
    }

    @Test
    void memoryEstimationWithSilhouette() {
        var graphDimensions = GraphDimensions.of(42, 1337);

        var parameters = mock(KmeansParameters.class);
        when(parameters.k()).thenReturn(10);
        when(parameters.computeSilhouette()).thenReturn(true);

        var memoryEstimation = new KmeansMemoryEstimateDefinition(parameters).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation).
            memoryRange(graphDimensions, new Concurrency(4))
            .hasMin(34304)
            .hasMax(55296);

    }

}
