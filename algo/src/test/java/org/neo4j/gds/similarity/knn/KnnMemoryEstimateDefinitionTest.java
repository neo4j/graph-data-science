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
package org.neo4j.gds.similarity.knn;

import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTree;
import org.neo4j.gds.mem.Estimate;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.mem.Estimate.sizeOfIntArray;
import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;
import static org.neo4j.gds.mem.Estimate.sizeOfOpenHashContainer;

class KnnMemoryEstimateDefinitionTest {

    static Stream<Arguments> smallParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(Arguments.of(1L), Arguments.of(10L), Arguments.of(100L), Arguments.of(10_00L)),
            () -> Stream.of(
                Arguments.of(KnnSampler.SamplerType.UNIFORM),
                Arguments.of(KnnSampler.SamplerType.RANDOMWALK)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("smallParameters")
    void memoryEstimationWithNodeProperty(long nodeCount, KnnSampler.SamplerType initialSampler) {
        var parameters = new KnnMemoryEstimationParametersBuilder(0.5, 10, initialSampler);
        var estimation = new KnnMemoryEstimateDefinition(parameters).memoryEstimation();
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        var concurrency = new Concurrency(1);
        MemoryTree estimate = estimation.estimate(dimensions, concurrency);
        MemoryRange actual = estimate.memoryUsage();

        assertEstimation(
            nodeCount,
            parameters.build(nodeCount).k(),
            initialSampler,
            actual
        );
    }

    static Stream<Arguments> largeParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(Arguments.of(1_000_000_000L), Arguments.of(100_000_000_000L)),
            () -> Stream.of(
                Arguments.of(KnnSampler.SamplerType.UNIFORM),
                Arguments.of(KnnSampler.SamplerType.RANDOMWALK)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("largeParameters")
    void memoryEstimationLargePagesWithProperty(long nodeCount, KnnSampler.SamplerType initialSampler) {
        var parameters = new KnnMemoryEstimationParametersBuilder(0.5, 10, initialSampler);
        var estimation = new KnnMemoryEstimateDefinition(parameters).memoryEstimation();
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        var concurrency = new Concurrency(1);
        MemoryTree estimate = estimation.estimate(dimensions, concurrency);
        MemoryRange actual = estimate.memoryUsage();

        assertEstimation(nodeCount, parameters.build(nodeCount).k(), initialSampler, actual);
    }

    private void assertEstimation(
        long nodeCount,
        K k,
        KnnSampler.SamplerType initialSampler,
        MemoryRange actual
    ) {
        long knnAlgo = Estimate.sizeOfInstance(Knn.class);

        long topKNeighborList = Estimate.sizeOfInstance(NeighborList.class) + sizeOfLongArray(k.value * 2L);
        long topKNeighborsList = HugeObjectArray.memoryEstimation(nodeCount, topKNeighborList);

        long tempNeighborsListMin = HugeObjectArray.memoryEstimation(nodeCount, 0);
        long tempNeighborsListMax = HugeObjectArray.memoryEstimation(
            nodeCount,
            Estimate.sizeOfInstance(LongArrayList.class) + sizeOfLongArray(k.sampledValue)
        );

        var randomList = KnnFactory.initialSamplerMemoryEstimation(initialSampler, k.value);
        long sampledList = sizeOfIntArray(sizeOfOpenHashContainer(k.sampledValue));

        long expectedMin = knnAlgo + topKNeighborsList + 4 * tempNeighborsListMin + randomList.min + sampledList;
        long expectedMax = knnAlgo + topKNeighborsList + 4 * tempNeighborsListMax + randomList.max + sampledList;

        assertEquals(expectedMin, actual.min);
        assertEquals(expectedMax, actual.max);
    }


}
