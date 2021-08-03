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
package org.neo4j.gds.triangle;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalClusteringCoefficientFactoryTest {

    @ValueSource(longs = {1L, 10L, 100L, 10_000L})
    @ParameterizedTest
    void memoryEstimation(long nodeCount) {
        MemoryEstimation estimation =
            new LocalClusteringCoefficientFactory<>().memoryEstimation(createConfig().build());

        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        long triangleCountEstimate = 56 + 24 + nodeCount * 8 + 16;
        long hugeDoubleArray = 24 + nodeCount * 8 + 16;
        long expected = 64 + hugeDoubleArray + triangleCountEstimate;

        assertEquals(expected, actual.min);
        assertEquals(expected, actual.max);
    }

    @ValueSource(longs = {1L, 10L, 100L, 10_000L})
    @ParameterizedTest
    void memoryEstimationWithSeedProperty(long nodeCount) {
        MemoryEstimation estimation =
            new LocalClusteringCoefficientFactory<>().memoryEstimation(createConfig().seedProperty("seed").build());

        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        long hugeDoubleArray = 24 + nodeCount * 8 + 16;
        long expected = 56 + hugeDoubleArray;
        assertEquals(expected, actual.min);
        assertEquals(expected, actual.max);
    }

    @CsvSource({"1000000000, 8001220736", "100000000000, 800122070336"})
    @ParameterizedTest
    void memoryEstimationLargePages(long nodeCount, long sizeOfHugeArray) {
        MemoryEstimation estimation =
            new LocalClusteringCoefficientFactory<>().memoryEstimation(createConfig().build());

        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        long triangleCountEstimate = 56 + 32 + sizeOfHugeArray;
        long hugeDoubleArray = 32 + sizeOfHugeArray;
        long expected = 64 + hugeDoubleArray + triangleCountEstimate;
        assertEquals(expected, actual.min);
        assertEquals(expected, actual.max);
    }

    @CsvSource({"1000000000, 8001220736", "100000000000, 800122070336"})
    @ParameterizedTest
    void memoryEstimationLargePagesWithSeed(long nodeCount, long sizeOfHugeArray) {
        MemoryEstimation estimation =
            new LocalClusteringCoefficientFactory<>().memoryEstimation(createConfig().seedProperty("seed").build());

        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        long hugeDoubleArray = 32 + sizeOfHugeArray;
        long expected = 56 + hugeDoubleArray;
        assertEquals(expected, actual.min);
        assertEquals(expected, actual.max);
    }

    private ImmutableLocalClusteringCoefficientBaseConfig.Builder createConfig() {
        return ImmutableLocalClusteringCoefficientBaseConfig.builder();
    }
}
