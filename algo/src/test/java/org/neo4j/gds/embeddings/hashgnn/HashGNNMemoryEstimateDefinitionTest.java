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

import java.util.List;
import java.util.Map;
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
        int iterations,
        long nodeCount,
        long relationshipCount,
        int concurrency,
        long expectedMinMemory,
        long expectedMaxMemory
    ) {

        var config=mock(HashGNNConfig.class);
        when(config.featureProperties()).thenReturn(List.of("f1","f2"));
        when(config.embeddingDensity()).thenReturn(embeddingDensity);
        when(config.iterations()).thenReturn(iterations);
        when(config.outputDimension()).thenReturn(Optional.empty());

        var memoryEstimation= new HashGNNMemoryEstimateDefinition().memoryEstimation(config);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount,relationshipCount,concurrency)
            .hasMin(expectedMinMemory)
            .hasMax(expectedMaxMemory);

    }

    @Test
    void shouldEstimateMemoryWithOutputDimension(){
//        Optional<Integer> maybeOutputDimension = Optional.of(outputDimension);
        //10,  4,  10_000, 20_000, 8, 100, 12_404_072, 12_404_072
        var config=mock(HashGNNConfig.class);
        when(config.featureProperties()).thenReturn(List.of("f1","f2"));
        when(config.embeddingDensity()).thenReturn(10);
        when(config.iterations()).thenReturn(4);
        when(config.outputDimension()).thenReturn(Optional.of(100));

        var memoryEstimation= new HashGNNMemoryEstimateDefinition().memoryEstimation(config);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(10_000,20_000,8)
            .hasSameMinAndMaxEqualTo(12_404_072);

    }


    @Test
    void estimationShouldUseGeneratedDimensionIfOutputIsMissing() {
        var inputDimension = 1000L;
        var inputRatio = 0.1;
        var graphDims = GraphDimensions.of((long) 1e6);
        var concurrency = 4;

        var bigEstimation = new HashGNNFactory<>()
            .memoryEstimation(HashGNNStreamConfigImpl
                .builder()
                .generateFeatures(Map.of("dimension", inputDimension, "densityLevel", 1))
                .iterations(3)
                .embeddingDensity(100)
                .build())
            .estimate(graphDims, concurrency)
            .memoryUsage();

        var smallEstimation = new HashGNNFactory<>()
            .memoryEstimation(HashGNNStreamConfigImpl
                .builder()
                .generateFeatures(Map.of("dimension", (long) (inputRatio * inputDimension), "densityLevel", 1))
                .iterations(3)
                .embeddingDensity(100)
                .build())
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
