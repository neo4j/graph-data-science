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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class LinkPredictionTrainFactoryTest {
    @Test
    @GdsEditionTest(Edition.EE)
    void shouldEstimateMemoryUsage() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5))
            .estimate(graphDimensions(10_100_000L, 70_000_000L, 30_000_000L), 4).memoryUsage().max;
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 100, 42))
            .estimate(graphDimensions(10_100_000L, 70_000_000L, 30_000_000L), 4).memoryUsage().max;
        var m3 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5))
            .estimate(graphDimensions(10_100_000L, 70_000_000L, 30_000_000L), 64).memoryUsage().max;
        var m4 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5000))
            .estimate(graphDimensions(10_100_000L, 70_000_000L, 30_000_000L), 4).memoryUsage().max;
        var m5 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5))
            .estimate(graphDimensions(10_100_000_000L, 70_000_000_000L, 30_000_000_000L), 4).memoryUsage().max;
        var m6 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5000))
            .estimate(graphDimensions(10_100_000_000L, 70_000_000_000L, 30_000_000_000L), 4).memoryUsage().max;
        var m7 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "COSINE", 10, 5))
            .estimate(graphDimensions(10_100_000_000L, 70_000_000_000L, 30_000_000_000L), 4).memoryUsage().max;
        var m8 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "COSINE", 10, 5000))
            .estimate(graphDimensions(10_100_000_000L, 70_000_000_000L, 30_000_000_000L), 4).memoryUsage().max;
        getClass();
    }

    private LinkPredictionTrainConfig getConfig(int concurrency, String linkFeatureCombiner, int batchSize, int nodeProperties) {
        return ImmutableLinkPredictionTrainConfig.builder()
            .modelName("model")
            .featureProperties(IntStream
                .range(0, nodeProperties)
                .boxed()
                .map(String::valueOf)
                .collect(Collectors.toList()))
            .addAllMetrics(List.of())
            .validationFolds(5)
            .negativeClassWeight(1.0)
            .trainRelationshipType(RelationshipType.of("TRAIN"))
            .testRelationshipType(RelationshipType.of("TEST"))
            .concurrency(concurrency)
            .params(
                List.of(
                    Map.of("linkFeatureCombiner", linkFeatureCombiner, "penalty", 1.0, "batchSize", batchSize)
                )
            )
            .build();
    }

    private GraphDimensions graphDimensions(long nodeCount, long trainRelationshipCount, long testRelationshipCount) {
        return ImmutableGraphDimensions
            .builder()
            .relationshipCounts(Map.of(
                RelationshipType.of("TRAIN"), trainRelationshipCount,
                RelationshipType.of("TEST"), testRelationshipCount
            ))
            .nodeCount(nodeCount)
            .build();
    }
}
