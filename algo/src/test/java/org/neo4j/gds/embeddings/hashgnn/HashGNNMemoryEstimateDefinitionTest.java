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
package org.neo4j.gds.embeddings.hashgnn;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HashGNNMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource(value = {
        // BASE
        "    10,  4,  10_000, 20_000, 1,   5_924_072, 86_324_072",
        // Should increase fairly little with higher density
        "   100,  4,  10_000, 20_000, 1,   7_038_992, 87_438_992",
        // Should increase fairly little with more iterations
        "    10, 16,  10_000, 20_000, 1,   5_924_072, 86_324_072",
        // Should increase almost linearly with node count
        "    10,  4, 100_000, 20_000, 1,  58_124_432, 862_124_432",
        // Should be unaffected by relationship count
        "    10,  4,  10_000, 80_000, 1,   5_924_072, 86_324_072",
        // Should be unaffected by concurrency
        "    10,  4,  10_000, 20_000, 8,  5_924_072, 86_324_072",
    })
    void shouldEstimateMemory(
        int embeddingDensity,
        int iterations, // seems iterations doesn't affect memory estimation. that's for someone else to investigate.
        long nodeCount,
        long relationshipCount,
        int concurrency,
        long expectedMinMemory,
        long expectedMaxMemory
    ) {

        var params = mock(HashGNNParameters.class);
        when(params.embeddingDensity()).thenReturn(embeddingDensity);
        when(params.heterogeneous()).thenReturn(false);
        when(params.outputDimension()).thenReturn(Optional.empty());
        when(params.generateFeatures()).thenReturn(Optional.empty());
        when(params.binarizeFeatures()).thenReturn(Optional.empty());

        var memoryEstimation = new HashGNNMemoryEstimateDefinition(params).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount,relationshipCount,new Concurrency(concurrency))
            .hasMin(expectedMinMemory)
            .hasMax(expectedMaxMemory);

    }

    @Test
    void shouldEstimateMemoryWithOutputDimension(){

        var params = mock(HashGNNParameters.class);
        when(params.embeddingDensity()).thenReturn(10);
        when(params.heterogeneous()).thenReturn(false);
        when(params.outputDimension()).thenReturn(Optional.of(100));
        when(params.generateFeatures()).thenReturn(Optional.empty());
        when(params.binarizeFeatures()).thenReturn(Optional.empty());

        var memoryEstimation = new HashGNNMemoryEstimateDefinition(params).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(10_000,20_000,new Concurrency(8))
            .hasSameMinAndMaxEqualTo(12_404_072);
    }

    @Test
    void estimationShouldUseGeneratedDimensionIfOutputIsMissing() {
        var inputDimension = 1000;
        var inputRatio = 0.1;
        var graphDims = GraphDimensions.of((long) 1e6);
        var concurrency = new Concurrency(4);

        var bigParameters = new HashGNNParameters(
            concurrency,
            3,
            100,
            1,
            List.of(),
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.of(GenerateFeaturesConfigImpl.builder().dimension(inputDimension).densityLevel(1).build()),
            Optional.empty()
        );
        var bigEstimation = new HashGNNFactory<>()
            .memoryEstimation(bigParameters)
            .estimate(graphDims, concurrency)
            .memoryUsage();

        var smallParameters = new HashGNNParameters(
            concurrency,
            3,
            100,
            1,
            List.of(),
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.of(GenerateFeaturesConfigImpl.builder().dimension((int) (inputRatio * inputDimension)).densityLevel(1).build()),
            Optional.empty()
        );
        var smallEstimation = new HashGNNFactory<>()
            .memoryEstimation(smallParameters)
            .estimate(graphDims, concurrency)
            .memoryUsage();

        var maxOutputRatio = (double) smallEstimation.max / bigEstimation.max;
        assertThat(maxOutputRatio).isCloseTo(inputRatio, Offset.offset(0.1));

        //Lower bound of the memory estimation is for bitSet.
        //upper bound is when all the features are double[].
        //It is a range because the non-context features need to be converted to double[],
        // while the context can remain as bitSet
        var minOutputRatio = (double) smallEstimation.min / bigEstimation.min;
        assertThat(minOutputRatio).isCloseTo(0.42, Offset.offset(0.01));
    }
}
