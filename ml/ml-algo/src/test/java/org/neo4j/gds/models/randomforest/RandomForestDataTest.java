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
package org.neo4j.gds.models.randomforest;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.GraphDimensions;

import static org.assertj.core.api.Assertions.assertThat;

class RandomForestDataTest {

    @ParameterizedTest
    @CsvSource(value = {
        // Max should almost scale linearly with numberOfDecisionTrees.
        "     6, 100_000,   1,  2,    96,     5_136",
        "     6, 100_000, 100,  2, 5_640,   509_640",
        // Max should increase with maxDepth when maxDepth limiting factor of trees' sizes.
        "    10, 100_000,   1,  2,    96,    81_936",
        // Max should scale almost inverse linearly with minSplitSize.
        "   800, 100_000,   1,  2,    96, 8_000_016",
        "   800, 100_000,   1, 10,    96, 1_600_016",
    })
    void memoryEstimation(
        int maxDepth,
        long numberOfTrainingSamples,
        int numTrees,
        int minSplitSize,
        long expectedMin,
        long expectedMax
    ) {
        var config = RandomForestTrainConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .minSplitSize(minSplitSize)
            .build();
        var estimator = RandomForestData.memoryEstimation(
            unused -> numberOfTrainingSamples,
            config
        );
        // Does not depend on node count, only indirectly so with the size of the training set.
        var estimation = estimator.estimate(GraphDimensions.of(10), 4).memoryUsage();

        assertThat(estimation.min).isEqualTo(expectedMin);
        assertThat(estimation.max).isEqualTo(expectedMax);
    }
}
