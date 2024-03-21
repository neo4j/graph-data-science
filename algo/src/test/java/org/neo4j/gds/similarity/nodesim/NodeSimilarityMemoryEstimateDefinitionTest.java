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
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeSimilarityMemoryEstimateDefinitionTest {


    @ParameterizedTest(name = "topK = {0}")
    @ValueSource(ints = {10, 100})
    void shouldComputeMemrec(int topK) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();

        var estimateParams = NodeSimilarityEstimateParameters.create(topK, 0, false, false, true);
        MemoryTree actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, 1);

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

        long topKMapRangeMin;
        long topKMapRangeMax;
        if (topK == 10) {
            topKMapRangeMin = 248_000_016L;
            topKMapRangeMax = 248_000_016L;
        } else {
            topKMapRangeMin = 1_688_000_016L;
            topKMapRangeMax = 1_688_000_016L;
        }
        builder.fixed("topK map", MemoryRange.of(topKMapRangeMin, topKMapRangeMax));

        MemoryTree expected = builder.build().estimate(dimensions, 1);

        assertEquals(expected.memoryUsage(), actual.memoryUsage());
    }

    @ParameterizedTest(name = "topK = {0}")
    @ValueSource(ints = {10, 100})
    void shouldComputeMemrecWithTop(int topK) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();

        var estimateParams = NodeSimilarityEstimateParameters.create(topK, 100, false, false, true);
        MemoryTree actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, 1);

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

        long topKMapRangeMin;
        long topKMapRangeMax;
        if (topK == 10) {
            topKMapRangeMin = 248_000_016L;
            topKMapRangeMax = 248_000_016L;
        } else {
            topKMapRangeMin = 1_688_000_016L;
            topKMapRangeMax = 1_688_000_016L;
        }
        builder.fixed("topK map", MemoryRange.of(topKMapRangeMin, topKMapRangeMax));

        MemoryTree expected = builder.build().estimate(dimensions, 1);

        assertEquals(expected.memoryUsage(), actual.memoryUsage());
    }

    @Test
    void shouldComputeMemrecWithTopKAndTopNGreaterThanNodeCount() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100)
            .relCountUpperBound(20_000)
            .build();

        var estimateParams = NodeSimilarityEstimateParameters.create(
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            false, false, true
        );

        MemoryTree actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, 1);

        assertEquals(570592, actual.memoryUsage().min);
        assertEquals(732192, actual.memoryUsage().max);

    }

    @ParameterizedTest(name = "componentProperty = {0}")
    @ValueSource(booleans = {true, false})
    void shouldComputeMemrecWithOrWithoutComponentMapping(boolean componentPropertySet) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(1_000_000)
            .relCountUpperBound(5_000_000)
            .build();


        var estimateParams = NodeSimilarityEstimateParameters
            .create(10, 0, true, !componentPropertySet, true);
        MemoryTree actual = new NodeSimilarityMemoryEstimateDefinition(estimateParams).memoryEstimation()
            .estimate(dimensions, 1);


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
        if (componentPropertySet) {
            builder.fixed("component mapping", 8000040);
        } else {
            builder.fixed("wcc", 8000064);
        }

        long topKMapRangeMin = 248_000_016L;
        long topKMapRangeMax = 248_000_016L;
        builder.fixed("topK map", MemoryRange.of(topKMapRangeMin, topKMapRangeMax));

        MemoryTree expected = builder.build().estimate(dimensions, 1);
        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(expected.memoryUsage().max).isEqualTo(actual.memoryUsage().max);
        softAssertions.assertThat(expected.memoryUsage().min).isEqualTo(actual.memoryUsage().min);

        softAssertions.assertAll();
    }

}
