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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.linkmodels.LinkPredictionTrainEstimation.ASSUMED_MIN_NODE_FEATURES;
import static org.neo4j.gds.MemoryEstimationTestUtil.subTree;

class LinkPredictionTrainFactoryTest {
    @Test
    @GdsEditionTest(Edition.EE)
    void nodeCountShouldAffectOnlyNodeIdsAndSplitsAndDoSoLinearlyWhenNodesDominate() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5))
            .estimate(graphDimensions(1_000_100L, 700L, 300L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, 5))
            .estimate(graphDimensions(10_100_000L, 700L, 300L), 4);
        var smallGraphUsage = m1.memoryUsage().max;
        var largeGraphUsage = m2.memoryUsage().max;
        long smallGraphNodeIdsUsage = subTree(m1, "node IDs").memoryUsage().max;
        long largeGraphNodeIdsUsage = subTree(m2, "node IDs").memoryUsage().max;
        long smallGraphSplitUsage = subTree(m1, List.of("max", "model selection", "split")).memoryUsage().max;
        long largeGraphSplitUsage = subTree(m2, List.of("max", "model selection", "split")).memoryUsage().max;
        assertThat(smallGraphNodeIdsUsage + smallGraphSplitUsage).isCloseTo(smallGraphUsage, Percentage.withPercentage(1));
        assertThat(largeGraphNodeIdsUsage + largeGraphSplitUsage).isCloseTo(largeGraphUsage, Percentage.withPercentage(1));
        assertThat(smallGraphNodeIdsUsage * 10).isCloseTo(largeGraphNodeIdsUsage, Percentage.withPercentage(1));
        assertThat(smallGraphSplitUsage * 10).isCloseTo(largeGraphSplitUsage, Percentage.withPercentage(1));
        assertThat(smallGraphUsage - smallGraphNodeIdsUsage - smallGraphSplitUsage).isEqualTo(largeGraphUsage - largeGraphNodeIdsUsage - largeGraphSplitUsage);
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void nodePropertiesShouldNotAffectWhenUsingCosine() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "COSINE", 10, 5))
            .estimate(graphDimensions(10_100L, 70_000L, 30_000L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "COSINE", 10, 500000))
            .estimate(graphDimensions(10_100L, 70_000L, 30_000L), 4);
        var fewNodePropertiesUsage = m1.memoryUsage().max;
        var manyNodePropertiesUsage = m2.memoryUsage().max;
        assertThat(fewNodePropertiesUsage).isEqualTo(manyNodePropertiesUsage);
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void nodePropertiesShouldAffectWhenNotUsingCosine() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, ASSUMED_MIN_NODE_FEATURES * 1000))
            .estimate(graphDimensions(10_100L, 70_000L, 30_000L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 10, ASSUMED_MIN_NODE_FEATURES * 10000))
            .estimate(graphDimensions(10_100L, 70_000L, 30_000L), 4);
        var fewNodePropertiesUsage = m1.memoryUsage().max;
        var manyNodePropertiesUsage = m2.memoryUsage().max;
        assertThat(10 * fewNodePropertiesUsage).isCloseTo(manyNodePropertiesUsage, Percentage.withPercentage(1));
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void batchSizeShouldScaleLinearlyWhenComputationGraphIsDominating() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 100000, 5))
            .estimate(graphDimensions(1_100L, 7_000L, 3_000L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "HADAMARD", 100000 * 10, 5))
            .estimate(graphDimensions(1_100L, 7_000L, 3_000L), 4);
        var smallBatchSizeUsage = m1.memoryUsage().max;
        var largeBatchSizeUsage = m2.memoryUsage().max;
        var path = List.of("max", "model selection", "max over models", "train and evaluate model", "max of train model and compute metrics", "train model", "computation graph");
        assertThat(smallBatchSizeUsage).isCloseTo(subTree(m1, path).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(largeBatchSizeUsage).isCloseTo(subTree(m2, path).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(10 * smallBatchSizeUsage).isCloseTo(largeBatchSizeUsage, Percentage.withPercentage(1));
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void concurrencyShouldScaleLinearlyWhenComputationGraphIsDominating() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "L2", 100000, 5))
            .estimate(graphDimensions(1_100L, 7_000L, 3_000L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "L2", 100000, 5))
            .estimate(graphDimensions(1_100L, 7_000L, 3_000L), 40);
        var lowConcurrencyUsage = m1.memoryUsage().max;
        var highConcurrencyUsage = m2.memoryUsage().max;
        var path = List.of("max", "model selection", "max over models", "train and evaluate model", "max of train model and compute metrics", "train model", "computation graph");
        assertThat(lowConcurrencyUsage).isCloseTo(subTree(m1, path).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(highConcurrencyUsage).isCloseTo(subTree(m2, path).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(10 * lowConcurrencyUsage).isCloseTo(highConcurrencyUsage, Percentage.withPercentage(1));
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void trainRelCountShouldScaleLinearlyWhenTrainMetricsDominate() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "L2", 100, 5))
            .estimate(graphDimensions(1_100L, 7_000_000L, 3_000L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "L2", 100, 5))
            .estimate(graphDimensions(1_100L, 70_000_000L, 7_000L), 40);
        var lowRelCountUsage = m1.memoryUsage().max;
        var highRelCountUsage = m2.memoryUsage().max;
        assertThat(lowRelCountUsage).isCloseTo(subTree(m1, List.of("max", "compute train metrics")).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(highRelCountUsage).isCloseTo(subTree(m2, List.of("max", "compute train metrics")).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(10 * lowRelCountUsage).isCloseTo(highRelCountUsage, Percentage.withPercentage(1));
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void testRelCountShouldScaleLinearlyWhenTestMetricsDominate() {
        var m1 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "L2", 100, 5))
            .estimate(graphDimensions(1_100L, 70_000L, 3_000_000L), 4);
        var m2 = new LinkPredictionTrainFactory().memoryEstimation(getConfig(4, "L2", 100, 5))
            .estimate(graphDimensions(1_100L, 7_000L, 30_000_000L), 40);
        var lowRelCountUsage = m1.memoryUsage().max;
        var highRelCountUsage = m2.memoryUsage().max;
        assertThat(lowRelCountUsage).isCloseTo(subTree(m1, List.of("max", "compute test metrics")).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(highRelCountUsage).isCloseTo(subTree(m2, List.of("max", "compute test metrics")).memoryUsage().max, Percentage.withPercentage(1));
        assertThat(10 * lowRelCountUsage).isCloseTo(highRelCountUsage, Percentage.withPercentage(1));
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
