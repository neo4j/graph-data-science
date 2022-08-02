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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KmeansAlgorithmFactoryTest {

    @Test
    void memoryEstimation() {
        var config = KmeansStreamConfig.of(CypherMapWrapper.create(Map.of(
            "nodeProperty",
            "i-am-a-fake-property"
        )));
        var graphDimensions = GraphDimensions.of(42, 1337);
        var memoryTree = new KmeansAlgorithmFactory<>()
            .memoryEstimation(config)
            .estimate(graphDimensions, 4);

        var estimate = memoryTree.memoryUsage();

        var sizeOfKMeansInstance = MemoryUsage.sizeOfInstance(Kmeans.class);
        var bestCommunitiesSize = 208L;
        var bestCentroidsSize = 10416L;
        var nodesInClusterSize = MemoryUsage.sizeOfLongArray(42);
        var distanceFromCentroid = HugeDoubleArray.memoryEstimation(42);
        var kMeansSize = sizeOfKMeansInstance + bestCommunitiesSize + bestCentroidsSize + nodesInClusterSize + distanceFromCentroid;

        var clusterManagerEstimate = ClusterManager.memoryEstimation(config.k(), 128).estimate(graphDimensions, 4).memoryUsage();
        var clusterManagerMin = clusterManagerEstimate.min;
        var clusterManagerMax = clusterManagerEstimate.max;

        var kMeansTaskEstimate = KmeansTask.memoryEstimation(config.k(), 128).estimate(graphDimensions, 4).memoryUsage();
        var kMeansTasksSizeMin = 4 * kMeansTaskEstimate.min;
        var kMeansTasksSizeMax = 4 * kMeansTaskEstimate.max;

        assertThat(estimate.min).isEqualTo(kMeansSize + clusterManagerMin + kMeansTasksSizeMin);
        assertThat(estimate.max).isEqualTo(kMeansSize + clusterManagerMax + kMeansTasksSizeMax);
    }

    @Test
    void memoryEstimationWithSeededCentroids() {
        var centroidsSeeds = List.of(List.of(40.712776, -74.005974), List.of(52.370216, 4.895168), List.of(41.902782, 12.496365));
        var config = KmeansStreamConfig.of(CypherMapWrapper.create(Map.of(
            "nodeProperty",
            "i-am-a-fake-property",
            "seedCentroids", centroidsSeeds
        )));
        var graphDimensions = GraphDimensions.of(42, 1337);
        var memoryTree = new KmeansAlgorithmFactory<>()
            .memoryEstimation(config)
            .estimate(graphDimensions, 4);

        var estimate = memoryTree.memoryUsage();

        var sizeOfKMeansInstance = MemoryUsage.sizeOfInstance(Kmeans.class);
        var bestCommunitiesSize = 208L;
        var bestCentroidsSize = 10416L;
        var nodesInClusterSize = MemoryUsage.sizeOfLongArray(42);
        var distanceFromCentroid = HugeDoubleArray.memoryEstimation(42);
        var clusterManagerEstimate = ClusterManager.memoryEstimation(config.k(), 128).estimate(graphDimensions, 4).memoryUsage();
        var clusterManagerMin = clusterManagerEstimate.min;
        var clusterManagerMax = clusterManagerEstimate.max;

        var kMeansTaskEstimate = KmeansTask.memoryEstimation(config.k(), 128).estimate(graphDimensions, 4).memoryUsage();
        var kMeansTasksSizeMin = 4 * kMeansTaskEstimate.min;
        var kMeansTasksSizeMax = 4 * kMeansTaskEstimate.max;

        var seedCentroidsSize = MemoryUsage.sizeOf(centroidsSeeds);

        var kMeansSize = sizeOfKMeansInstance + bestCommunitiesSize + bestCentroidsSize + nodesInClusterSize + distanceFromCentroid + seedCentroidsSize;
        assertThat(estimate.min).isEqualTo(kMeansSize + clusterManagerMin + kMeansTasksSizeMin);
        assertThat(estimate.max).isEqualTo(kMeansSize + clusterManagerMax + kMeansTasksSizeMax);
    }

    @Test
    void memoryEstimationWithSilhouette() {
        var config = KmeansStreamConfig.of(CypherMapWrapper.create(Map.of(
            "nodeProperty",
            "i-am-a-fake-property",
            "computeSilhouette",
            true
        )));
        var graphDimensions = GraphDimensions.of(42, 1337);
        var memoryTree = new KmeansAlgorithmFactory<>()
            .memoryEstimation(config)
            .estimate(graphDimensions, 4);

        var estimate = memoryTree.memoryUsage();

        var sizeOfKMeansInstance = MemoryUsage.sizeOfInstance(Kmeans.class);
        var bestCommunitiesSize = 208L;
        var bestCentroidsSize = 10416L;
        var nodesInClusterSize = MemoryUsage.sizeOfLongArray(42);
        var distanceFromCentroid = HugeDoubleArray.memoryEstimation(42);

        var clusterManagerEstimate = ClusterManager.memoryEstimation(config.k(), 128).estimate(graphDimensions, 4).memoryUsage();
        var clusterManagerMin = clusterManagerEstimate.min;
        var clusterManagerMax = clusterManagerEstimate.max;

        var kMeansTaskEstimate = KmeansTask.memoryEstimation(config.k(), 128).estimate(graphDimensions, 4).memoryUsage();
        var kMeansTasksSizeMin = 4 * kMeansTaskEstimate.min;
        var kMeansTasksSizeMax = 4 * kMeansTaskEstimate.max;

        var silhouetteSize = HugeDoubleArray.memoryEstimation(42);
        var kMeansSize = sizeOfKMeansInstance + bestCommunitiesSize + bestCentroidsSize + nodesInClusterSize + distanceFromCentroid + silhouetteSize;
        assertThat(estimate.min).isEqualTo(kMeansSize + clusterManagerMin + kMeansTasksSizeMin);
        assertThat(estimate.max).isEqualTo(kMeansSize + clusterManagerMax + kMeansTasksSizeMax);
    }

}
