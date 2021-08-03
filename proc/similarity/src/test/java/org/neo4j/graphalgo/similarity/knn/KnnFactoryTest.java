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
package org.neo4j.graphalgo.similarity.knn;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfOpenHashContainer;

class KnnFactoryTest {

    @ParameterizedTest
    @ValueSource(longs = {1L, 10L, 100L, 10_000L})
    void memoryEstimationWithNodeProperty(long nodeCount) {
        var config = knnConfig();
        var boundedK = config.boundedK(nodeCount);
        var sampledK = config.sampledK(nodeCount);

        MemoryEstimation estimation = new KnnFactory<>().memoryEstimation(config);
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        assertEstimation(nodeCount, 24, sizeOfObjectArray(nodeCount), boundedK, sampledK, actual);
    }

    @ParameterizedTest
    @ValueSource(longs = {1000000000L, 100000000000L})
    void memoryEstimationLargePagesWithProperty(long nodeCount) {
        var config = knnConfig();
        var boundedK = config.boundedK(nodeCount);
        var sampledK = config.sampledK(nodeCount);

        MemoryEstimation estimation = new KnnFactory<>().memoryEstimation(config);
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        MemoryTree estimate = estimation.estimate(dimensions, 1);
        MemoryRange actual = estimate.memoryUsage();

        int pageSize = 16384;
        int numPages = (int) ceilDiv(nodeCount, 16384);
        var sizeOfHugeArray = sizeOfObjectArray(numPages) + numPages * sizeOfObjectArray(pageSize);
        assertEstimation(nodeCount, 32, sizeOfHugeArray, boundedK, sampledK, actual);
    }

    private void assertEstimation(
        long nodeCount,
        int sizeOfHugeArrayInstance,
        long sizeOfHugeArray,
        int boundedK,
        int sampledK,
        MemoryRange actual
    ) {
        long knnAlgo = /* KNN */ 48;

        long topKNeighborList = /* NL */ 24 + sizeOfLongArray(boundedK * 2);
        long topKNeighborsList = /* HOA */ sizeOfHugeArrayInstance + sizeOfHugeArray + nodeCount * topKNeighborList;

        long tempNeighborsListMin = /* HOA */ sizeOfHugeArrayInstance + sizeOfHugeArray;
        long tempNeighborsListMax = tempNeighborsListMin + nodeCount * (/* LAL */ 24 + sizeOfLongArray(sampledK));

        long randomList = sizeOfLongArray(sizeOfOpenHashContainer(boundedK));
        long sampledList = sizeOfIntArray(sizeOfOpenHashContainer(sampledK));

        long expectedMin = knnAlgo + topKNeighborsList + 4 * tempNeighborsListMin + randomList + sampledList;
        long expectedMax = knnAlgo + topKNeighborsList + 4 * tempNeighborsListMax + randomList + sampledList;

        assertEquals(expectedMin, actual.min);
        assertEquals(expectedMax, actual.max);
    }

    private KnnBaseConfig knnConfig() {
        return ImmutableKnnBaseConfig.builder().nodeWeightProperty("knn").build();
    }


}
