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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationTrainAlgorithmFactoryTest {
    @Test
    @GdsEditionTest(Edition.EE)
    void shouldEstimateMemoryUsage() {
        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .targetProperty("target")
            .addAllMetrics(List.of())
            .holdoutFraction(0.2)
            .validationFolds(5)
            .concurrency(64)
            .params(
                List.of(
                    Map.of("penalty", 1.0, "batchSize", 100),
                    Map.of("penalty", 2.0, "batchSize", 1000),
                    Map.of("penalty", 3.0, "batchSize", 10000),
                    Map.of("penalty", 4.0, "batchSize", 10),
                    Map.of("penalty", 5.0, "batchSize", 110),
                    Map.of("penalty", 6.0, "batchSize", 110),
                    Map.of("penalty", 7.0, "batchSize", 1020)
                )
            )
            .build();

        // at small scales and little concurrency, storing nodes matters most
        var estimateOnSmallishGraph = new NodeClassificationTrainAlgorithmFactory()
            .memoryEstimation(config)
            .estimate(GraphDimensions.of(10_000_000L, 100_000_000L), 4);
        assertThat(estimateOnSmallishGraph.memoryUsage().max).isCloseTo(2100L * 1024 * 1024, Percentage.withPercentage(8));

        // as we up concurrency at this scale, the actual training dominates
        var estimateOnSmallishGraphWithHighConcurrency = new NodeClassificationTrainAlgorithmFactory()
            .memoryEstimation(config)
            .estimate(GraphDimensions.of(10_000_000L, 100_000_000L), 64);

        assertThat(estimateOnSmallishGraphWithHighConcurrency.memoryUsage().max).isCloseTo(24L * 1024 * 1024 * 1024, Percentage.withPercentage(8));

        // as number of nodes grows, they start to dominate
        var estimateOnLargeGraph = new NodeClassificationTrainAlgorithmFactory()
            .memoryEstimation(config)
            .estimate(GraphDimensions.of(10_000_000_000L, 100_000_000_000L), 64);
        assertThat(estimateOnLargeGraph.memoryUsage().max).isCloseTo(550L * 1024 * 1024 * 1024, Percentage.withPercentage(8));
    }
}
