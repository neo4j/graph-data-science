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
package org.neo4j.gds.ml.decisiontree;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SplitMeanSquareErrorTest {

    private static Stream<Arguments> groupParameters() {
        return Stream.of(
            Arguments.of(
                HugeDoubleArray.of(1, 5),
                HugeLongArray.of(0, 1),
                0,
                2,
                4,
                6,
                26
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 5),
                HugeLongArray.of(0, 1, 2, 3),
                0,
                2,
                4,
                6,
                26
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 5),
                HugeLongArray.of(),
                0,
                0,
                0,
                0,
                0
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 3.2, 12.9, 5),
                HugeLongArray.of(0, 1, 2, 3),
                0,
                4,
                20.136875,
                22.1,
                202.65
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 3.2, 12.9, 5),
                HugeLongArray.of(2, 1, 3, 0),
                0,
                4,
                20.136875,
                22.1,
                202.65
            )
        );
    }

    @ParameterizedTest
    @MethodSource("groupParameters")
    void shouldComputeCorrectGroupMetaData(
        HugeDoubleArray targets,
        HugeLongArray group,
        long startIdx,
        long size,
        double expectedImpurity,
        double expectedSum,
        double expectedSumOfSquares
    ) {
        var mseLoss = new SplitMeanSquaredError(targets);
        var impurityData = mseLoss.groupImpurity(group, startIdx, size);

        assertThat(impurityData.impurity())
            .isCloseTo(expectedImpurity, Offset.offset(0.00001D));
        assertThat(impurityData.sum()).isCloseTo(expectedSum, Offset.offset(0.00001D));
        assertThat(impurityData.sumOfSquares()).isCloseTo(expectedSumOfSquares, Offset.offset(0.00001D));
        assertThat(impurityData.groupSize()).isEqualTo(size);
    }

    private static Stream<Arguments> incrementParameters() {
        return Stream.of(
            Arguments.of(
                1,
                4,
                119.506875,
                37.3,
                825.85
            ),
            Arguments.of(
                2,
                4,
                107.4425,
                47,
                982.02
            )
        );
    }

    @ParameterizedTest
    @MethodSource("incrementParameters")
    void shouldComputeCorrectIncrementalMetaData(
        long featureVectorIdx,
        long size,
        double expectedImpurity,
        double expectedSum,
        double expectedSumOfSquares
    ) {
        var mseLoss = new SplitMeanSquaredError(HugeDoubleArray.of(1, 3.2, 12.9, 5, 28.1));
        var impurityData = mseLoss.groupImpurity(HugeLongArray.of(0, 4, 3), 0, 3);
        mseLoss.incrementalImpurity(featureVectorIdx, impurityData);

        assertThat(impurityData.impurity())
            .isCloseTo(expectedImpurity, Offset.offset(0.00001D));
        assertThat(impurityData.sum()).isCloseTo(expectedSum, Offset.offset(0.00001D));
        assertThat(impurityData.sumOfSquares()).isCloseTo(expectedSumOfSquares, Offset.offset(0.00001D));
        assertThat(impurityData.groupSize()).isEqualTo(size);
    }

    private static Stream<Arguments> decrementParameters() {
        return Stream.of(
            Arguments.of(
                2,
                4,
                119.506875,
                37.3,
                825.85
            ),
            Arguments.of(
                1,
                4,
                107.4425,
                47,
                982.02
            )
        );
    }

    @ParameterizedTest
    @MethodSource("decrementParameters")
    void shouldComputeCorrectDecrementalMetaData(
        long featureVectorIdx,
        long size,
        double expectedImpurity,
        double expectedSum,
        double expectedSumOfSquares
    ) {
        var mseLoss = new SplitMeanSquaredError(HugeDoubleArray.of(1, 3.2, 12.9, 5, 28.1));
        var impurityData = mseLoss.groupImpurity(HugeLongArray.of(0, 2, 1, 4, 3), 0, 5);
        mseLoss.decrementalImpurity(featureVectorIdx, impurityData);

        assertThat(impurityData.impurity())
            .isCloseTo(expectedImpurity, Offset.offset(0.00001D));
        assertThat(impurityData.sum()).isCloseTo(expectedSum, Offset.offset(0.00001D));
        assertThat(impurityData.sumOfSquares()).isCloseTo(expectedSumOfSquares, Offset.offset(0.00001D));
        assertThat(impurityData.groupSize()).isEqualTo(size);
    }

    @Test
    void shouldEstimateMemory() {
        assertThat(SplitMeanSquaredError.memoryEstimation())
            .isEqualTo(MemoryRange.of(16));
    }
}
