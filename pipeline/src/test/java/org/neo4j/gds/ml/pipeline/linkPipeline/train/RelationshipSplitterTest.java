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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;

import java.util.List;

import static org.neo4j.gds.TestSupport.assertMemoryRange;

class RelationshipSplitterTest {

    @Test
    void estimateWithDifferentTestFraction() {
        var splitConfigBuilder = LinkPredictionSplitConfigImpl.builder()
            .trainFraction(0.3)
            .validationFolds(3)
            .negativeSamplingRatio(1.0);

        var splitConfig = splitConfigBuilder.testFraction(0.2).build();
        var actualEstimation = RelationshipSplitter.splitEstimation(splitConfig, List.of("*"))
            .estimate(splitConfig.expectedGraphDimensions(100, 1_000), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(28_800, 35_840));

        splitConfig = splitConfigBuilder.testFraction(0.8).build();
        actualEstimation = RelationshipSplitter.splitEstimation(splitConfig, List.of("*"))
            .estimate(splitConfig.expectedGraphDimensions(100, 1_000), 4);

        // higher testFraction -> lower estimation as test-complement is smaller
        // the test_complement is kept until the end of all splitting
        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(19_152, 32_912));
    }

    @Test
    void estimateWithDifferentTrainFraction() {
        var splitConfigBuilder = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(3)
            .negativeSamplingRatio(1.0);

        var splitConfig = splitConfigBuilder.trainFraction(0.2).build();
        var actualEstimation = RelationshipSplitter.splitEstimation(splitConfig, List.of("*"))
            .estimate(splitConfig.expectedGraphDimensions(100, 1_000), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(27_200, 34_240));

        splitConfig = splitConfigBuilder.trainFraction(0.8).build();
        actualEstimation = RelationshipSplitter.splitEstimation(splitConfig, List.of("*"))
            .estimate(splitConfig.expectedGraphDimensions(100, 1_000), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(27_184, 40_944));
    }

    @Test
    void estimateWithDifferentNegativeSampling() {
        var splitConfigBuilder = LinkPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .trainFraction(0.3)
            .validationFolds(3);

        var splitConfig = splitConfigBuilder.negativeSamplingRatio(1).build();
        var actualEstimation = RelationshipSplitter.splitEstimation(splitConfig, List.of("*"))
            .estimate(splitConfig.expectedGraphDimensions(100, 1_000), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(27184, 35_344));

        splitConfig = splitConfigBuilder.negativeSamplingRatio(4).build();
        actualEstimation = RelationshipSplitter.splitEstimation(splitConfig, List.of("*"))
            .estimate(splitConfig.expectedGraphDimensions(100, 1_000), 4);

        assertMemoryRange(actualEstimation.memoryUsage(), MemoryRange.of(39424, 59_824));
    }
}
