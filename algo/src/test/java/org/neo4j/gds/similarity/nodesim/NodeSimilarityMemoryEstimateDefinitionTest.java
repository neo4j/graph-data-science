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
package org.neo4j.gds.similarity.nodesim;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

class NodeSimilarityMemoryEstimateDefinitionTest {


    @ParameterizedTest
    @CsvSource({"10, 248000016", "100, 1688000016"})
    void shouldComputeMemrec(int topK, long expectedTopKMemory) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();

        var estimateParams = new NodeSimilarityEstimateParameters(topK, 0, false, false, true);
        var concurrency = new Concurrency(1);
        var actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, concurrency).memoryUsage();

        long nodeFilterRangeMin = 125_016L;
        long nodeFilterRangeMax = 125_016L;
        MemoryRange nodeFilterRange = MemoryRange.of(nodeFilterRangeMin, nodeFilterRangeMax);

        long vectorsRangeMin = 56_000_016L;
        long vectorsRangeMax = 56_000_016L;
        MemoryRange vectorsRange = MemoryRange.of(vectorsRangeMin, vectorsRangeMax);

        long weightsRangeMin = 16L;
        long weightsRangeMax = 56_000_016L;
        MemoryRange weightsRange = MemoryRange.of(weightsRangeMin, weightsRangeMax);

        MemoryEstimations.Builder builder = MemoryEstimations.builder()
            .fixed("node filter", nodeFilterRange)
            .fixed("vectors", vectorsRange)
            .fixed("weights", weightsRange)
            .fixed("similarityComputer", 8);


        builder.fixed("topK map", MemoryRange.of(expectedTopKMemory));
        var expected = builder.build().estimate(dimensions, concurrency).memoryUsage();

        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(actual.max).isEqualTo(expected.max);
        softAssertions.assertThat(actual.min).isEqualTo(expected.min);
        softAssertions.assertAll();

    }

    @ParameterizedTest
    @CsvSource({"10, 248000016", "100, 1688000016"})
    void shouldComputeMemrecWithTop(int topK, long expectedTopKMemory) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();

        var estimateParams = new NodeSimilarityEstimateParameters(topK, 100, false, false, true);
        var concurrency = new Concurrency(1);
        var actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, concurrency).memoryUsage();

        long nodeFilterRangeMin = 125_016L;
        long nodeFilterRangeMax = 125_016L;
        MemoryRange nodeFilterRange = MemoryRange.of(nodeFilterRangeMin, nodeFilterRangeMax);

        long vectorsRangeMin = 56_000_016L;
        long vectorsRangeMax = 56_000_016L;
        MemoryRange vectorsRange = MemoryRange.of(vectorsRangeMin, vectorsRangeMax);

        long weightsRangeMin = 16L;
        long weightsRangeMax = 56_000_016L;
        MemoryRange weightsRange = MemoryRange.of(weightsRangeMin, weightsRangeMax);

        long topNListMin = 2_504L;
        long topNListMax = 2_504L;
        MemoryRange topNListRange = MemoryRange.of(topNListMin, topNListMax);

        MemoryEstimations.Builder builder = MemoryEstimations.builder()
            .fixed("node filter", nodeFilterRange)
            .fixed("vectors", vectorsRange)
            .fixed("weights", weightsRange)
            .fixed("topNList", topNListRange)
            .fixed("similarityComputer", 8);

        builder.fixed("topK map", MemoryRange.of(expectedTopKMemory));

        var expected = builder.build().estimate(dimensions, concurrency).memoryUsage();

        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(actual.max).isEqualTo(expected.max);
        softAssertions.assertThat(actual.min).isEqualTo(expected.min);
        softAssertions.assertAll();
    }

    @Test
    void shouldComputeMemrecWithTopKAndTopNGreaterThanNodeCount() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100)
            .relCountUpperBound(20_000)
            .build();

        var estimateParams = new NodeSimilarityEstimateParameters(
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            false, false, true
        );

        var concurrency = new Concurrency(1);
        var actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, concurrency).memoryUsage();

        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(actual.min).isEqualTo(570592);
        softAssertions.assertThat(actual.max).isEqualTo(732192);
        softAssertions.assertAll();

    }

    @ParameterizedTest(name = "componentProperty = {0}")
    @CsvSource({"true, 8000040", "false, 8000064 "})
    void shouldComputeMemrecWithOrWithoutComponentMapping(boolean componentPropertySet, long extraMemory) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();


        var estimateParams = new NodeSimilarityEstimateParameters(10, 0, true, !componentPropertySet, true);
        var concurrency = new Concurrency(1);
        var actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, concurrency).memoryUsage();


        long nodeFilterRangeMin = 125_016L;
        long nodeFilterRangeMax = 125_016L;
        MemoryRange nodeFilterRange = MemoryRange.of(nodeFilterRangeMin, nodeFilterRangeMax);

        long vectorsRangeMin = 56_000_016L;
        long vectorsRangeMax = 56_000_016L;
        MemoryRange vectorsRange = MemoryRange.of(vectorsRangeMin, vectorsRangeMax);

        long weightsRangeMin = 16L;
        long weightsRangeMax = 56_000_016L;
        MemoryRange weightsRange = MemoryRange.of(weightsRangeMin, weightsRangeMax);

        MemoryEstimations.Builder builder = MemoryEstimations.builder()
            .fixed("upper bound per component", 8000040)
            .fixed("nodes sorted by component", 8000040)
            .fixed("node filter", nodeFilterRange)
            .fixed("vectors", vectorsRange)
            .fixed("weights", weightsRange)
            .fixed("similarityComputer", 8);

        builder.fixed("extra", extraMemory);

        builder.fixed("topK map", MemoryRange.of(248_000_016L));

        var expected = builder.build().estimate(dimensions, concurrency).memoryUsage();

        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(actual.max).isEqualTo(expected.max);
        softAssertions.assertThat(actual.min).isEqualTo(expected.min);
        softAssertions.assertAll();
    }

}
