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

        assertThat(estimate.min)
            .as("Min should be correct")
            .isEqualTo(33944L);
        assertThat(estimate.max)
            .as("Max should be correct")
            .isEqualTo(54936L);
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

        assertThat(estimate.min)
            .as("Min should be correct")
            .isEqualTo(33944L + MemoryUsage.sizeOf(centroidsSeeds));
        assertThat(estimate.max)
            .as("Max should be correct")
            .isEqualTo(54936L + MemoryUsage.sizeOf(centroidsSeeds));
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

        assertThat(estimate.min)
            .as("Min should be correct")
            .isEqualTo(34320L);
        assertThat(estimate.max)
            .as("Max should be correct")
            .isEqualTo(55312L);
    }

}
