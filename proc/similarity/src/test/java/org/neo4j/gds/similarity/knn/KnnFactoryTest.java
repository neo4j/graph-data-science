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
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfOpenHashContainer;

class KnnFactoryTest {

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
        var config = knnConfig(initialSampler);
        var k = config.k(nodeCount);

        MemoryEstimation estimation = new KnnFactory<>().memoryEstimation(config);
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        assertEstimation(
            nodeCount,
            k.value,
            k.sampledValue,
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
        var config = knnConfig(initialSampler);
        var k = config.k(nodeCount);

        MemoryEstimation estimation = new KnnFactory<>().memoryEstimation(config);
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        assertEstimation(nodeCount, k.value, k.sampledValue, initialSampler, actual);
    }

    private void assertEstimation(
        long nodeCount,
        int boundedK,
        int sampledK,
        KnnSampler.SamplerType initialSampler,
        MemoryRange actual
    ) {
        long knnAlgo = MemoryUsage.sizeOfInstance(Knn.class);

        long topKNeighborList = MemoryUsage.sizeOfInstance(NeighborList.class) + sizeOfLongArray(boundedK * 2L);
        long topKNeighborsList = HugeObjectArray.memoryEstimation(nodeCount, topKNeighborList);

        long tempNeighborsListMin = HugeObjectArray.memoryEstimation(nodeCount, 0);
        long tempNeighborsListMax = HugeObjectArray.memoryEstimation(
            nodeCount,
            MemoryUsage.sizeOfInstance(LongArrayList.class) + sizeOfLongArray(sampledK)
        );

        var randomList = KnnFactory.initialSamplerMemoryEstimation(initialSampler, boundedK);
        long sampledList = sizeOfIntArray(sizeOfOpenHashContainer(sampledK));

        long expectedMin = knnAlgo + topKNeighborsList + 4 * tempNeighborsListMin + randomList.min + sampledList;
        long expectedMax = knnAlgo + topKNeighborsList + 4 * tempNeighborsListMax + randomList.max + sampledList;

        assertEquals(expectedMin, actual.min);
        assertEquals(expectedMax, actual.max);
    }

    private KnnBaseConfig knnConfig(KnnSampler.SamplerType initialSampler) {
        return KnnBaseConfigImpl.builder()
            .nodeProperties(KnnNodePropertySpecParser.parse("knn"))
            .initialSampler(initialSampler)
            .similarityCutoff(0.8)
            .build();
    }

}
