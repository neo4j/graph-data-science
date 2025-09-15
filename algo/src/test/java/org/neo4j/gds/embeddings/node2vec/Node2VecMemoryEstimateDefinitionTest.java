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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Node2VecMemoryEstimateDefinitionTest {

    @Test
    void shouldEstimateMemory() {

        var trainParams = mock(TrainParameters.class);
        when(trainParams.embeddingDimension()).thenReturn(128);
        var samplingWalkParameters = new SamplingWalkParameters(null,10, 80, 1.0, 1.0,1,1,1);

        var params = mock(Node2VecParameters.class);
        when(params.samplingWalkParameters()).thenReturn(samplingWalkParameters);
        when(params.trainParameters()).thenReturn(trainParams);

        var memoryEstimation = new Node2VecMemoryEstimateDefinition(params).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(1000, new Concurrency(1))
            .hasSameMinAndMaxEqualTo(7_688_472L);
    }

}
