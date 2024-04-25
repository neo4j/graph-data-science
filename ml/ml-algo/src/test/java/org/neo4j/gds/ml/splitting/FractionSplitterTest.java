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
package org.neo4j.gds.ml.splitting;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;

class FractionSplitterTest {
    @Test
    void shouldGiveEmptySets() {
        var fractionSplitter = new FractionSplitter();
        var split = fractionSplitter.split(ReadOnlyHugeLongArray.of(), 0.5);
        assertThat(split.trainSet().toArray()).isEmpty();
        assertThat(split.testSet().toArray()).isEmpty();
    }

    @Test
    void shouldGiveCorrectFractionConsecutiveIds() {
        double fraction = 0.65;
        var fractionSplitter = new FractionSplitter();
        var split = fractionSplitter.split(ReadOnlyHugeLongArray.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), fraction);

        assertThat(split.trainSet().toArray()).containsExactly(0, 1, 2, 3, 4, 5);
        assertThat(split.testSet().toArray()).containsExactly(6, 7, 8, 9);
    }


    @Test
    void shouldGiveCorrectIdsForGeneralInput() {
        var input = ReadOnlyHugeLongArray.of(4, 2, 1, 3, 3, 7);
        var split = new FractionSplitter().split(input, 0.6);

        assertThat(split.trainSet().toArray()).containsExactly(4, 2, 1);
        assertThat(split.testSet().toArray()).containsExactly(3, 3, 7);
    }

    @Test
    void splittingNodesDifferentlyShouldNotAffectMemoryEstimation() {
        var concurrency = new Concurrency(1);
        assertThat(
            FractionSplitter.estimate(0.1)
                .estimate(GraphDimensions.of(1000), concurrency)
                .memoryUsage()
        ).isEqualTo(
            FractionSplitter.estimate(0.9)
                .estimate(GraphDimensions.of(1000), concurrency)
                .memoryUsage()
        ).isEqualTo(
            FractionSplitter.estimate(0.45)
                .estimate(GraphDimensions.of(1000), concurrency)
                .memoryUsage()
        );
    }

    @Test
    void minAndMaxEstimationShouldBeTheSame() {
        var range = FractionSplitter.estimate(0.1)
            .estimate(GraphDimensions.of(1000), new Concurrency(1))
            .memoryUsage();
        assertThat(range.min).isEqualTo(range.max);
    }

    @Test
    void estimationIsNotAffectedByConcurrency() {
        var dimensions = GraphDimensions.of(1000);
        var estimator = FractionSplitter.estimate(0.1);
        assertThat(
            estimator.estimate(dimensions, new Concurrency(1)).memoryUsage()
        ).isEqualTo(
            estimator.estimate(dimensions, new Concurrency(4)).memoryUsage()
        ).isEqualTo(
            estimator.estimate(dimensions, new Concurrency(16)).memoryUsage()
        );
    }

    @Test
    void estimationShouldScaleWithNodeCount() {
        var baseNodeCount = 1000;
        var trainFraction = 0.1;
        var estimator = FractionSplitter.estimate(trainFraction);
        var concurrency = new Concurrency(1);
        var estimation = estimator
            .estimate(GraphDimensions.of(baseNodeCount), concurrency)
            .memoryUsage();
        var tolerance = 100L;

        // our test assumes max == min. we could inline the test that proves this, but coupled tests smell and checking is cheap
        assertThat(estimation.max).isEqualTo(
            estimator.estimate(GraphDimensions.of(baseNodeCount), concurrency).memoryUsage().min
        );

        assertThat(estimation.max)
            .isCloseTo(
                FractionSplitter.estimate(trainFraction)
                    .estimate(GraphDimensions.of(baseNodeCount * 10), concurrency)
                    .memoryUsage().max / 10, // should be ten times
                Offset.offset(tolerance)     // with some give
            ).isCloseTo(
                FractionSplitter.estimate(trainFraction)
                    .estimate(GraphDimensions.of(baseNodeCount * 100), concurrency)
                    .memoryUsage().max / 100, // should be a hundred times
                Offset.offset(tolerance)      // with some give
        );
    }

}
