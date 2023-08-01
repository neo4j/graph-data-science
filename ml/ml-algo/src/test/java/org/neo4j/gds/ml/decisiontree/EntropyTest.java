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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class EntropyTest {

    private static Stream<Arguments> groupParameters() {
        return Stream.of(
            Arguments.of(
                HugeIntArray.of(1, 0),
                HugeLongArray.of(0, 1),
                0,
                2,
                1.0D,
                new long[]{1, 1}
            ),
            Arguments.of(
                HugeIntArray.of(1, 0),
                HugeLongArray.of(0, 1),
                0,
                1,
                0.0D,
                new long[]{0, 1}
            ),
            Arguments.of(
                HugeIntArray.of(1, 0),
                HugeLongArray.of(),
                0,
                0,
                0.0D,
                new long[]{0, 0}
            ),
            Arguments.of(
                HugeIntArray.of(1, 1, 0, 0),
                HugeLongArray.of(0, 1, 2, 3),
                0,
                4,
                1.0D,
                new long[]{2, 2}
            ),
            Arguments.of(
                HugeIntArray.of(1, 1, 0, 0, 1, 1),
                HugeLongArray.of(0, 1, 2, 3, 4, 5),
                0,
                4,
                1.0D,
                new long[]{2, 2}
            ),
            Arguments.of(
                HugeIntArray.of(1, 1, 0, 0, 1, 1),
                HugeLongArray.of(0, 1, 2, 3, 4, 5),
                0,
                6,
                0.9182958340544894D,
                new long[]{2, 4}
            ),
            Arguments.of(
                HugeIntArray.of(1, 0, 0),
                HugeLongArray.of(0, 1, 2),
                0,
                3,
                0.9182958340544894D,
                new long[]{2, 1}
            ),
            Arguments.of(
                HugeIntArray.of(1, 0, 0),
                HugeLongArray.of(1, 2, 0),
                0,
                3,
                0.9182958340544894D,
                new long[]{2, 1}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("groupParameters")
    void shouldComputeCorrectGroupMetaData(
        HugeIntArray labels,
        HugeLongArray group,
        long startIdx,
        long size,
        double expectedImpurity,
        long[] expectedClassCounts
    ) {
        var entropyLossLoss = new Entropy(labels, 2);
        var impurityData = entropyLossLoss.groupImpurity(group, startIdx, size);

        assertThat(impurityData.impurity())
            .isCloseTo(expectedImpurity, Offset.offset(0.00001D));
        assertThat(impurityData.classCounts()).containsExactly(expectedClassCounts);
        assertThat(impurityData.groupSize()).isEqualTo(size);
    }

    private static Stream<Arguments> incrementParameters() {
        return Stream.of(
            Arguments.of(
                1,
                4,
                0.8112781244591326,
                new long[]{3, 1}
            ),
            Arguments.of(
                4,
                4,
                1.0D,
                new long[]{2, 2}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("incrementParameters")
    void shouldComputeCorrectIncrementalMetaData(
        long featureVectorIdx,
        long size,
        double expectedLoss,
        long[] expectedClassCounts
    ) {
        var labels = HugeIntArray.of(1, 0, 0, 0, 1);
        var entropyLossLoss = new Entropy(labels, 2);
        var impurityData = entropyLossLoss.groupImpurity(HugeLongArray.of(2, 0, 3), 0, 3);
        entropyLossLoss.incrementalImpurity(featureVectorIdx, impurityData);

        assertThat(impurityData.impurity())
            .isCloseTo(expectedLoss, Offset.offset(0.00001D));
        assertThat(impurityData.classCounts()).containsExactly(expectedClassCounts);
        assertThat(impurityData.groupSize()).isEqualTo(size);
    }

    private static Stream<Arguments> decrementParameters() {
        return Stream.of(
            Arguments.of(
                1,
                4,
                1.0D,
                new long[]{2, 2}
            ),
            Arguments.of(
                4,
                4,
                0.8112781244591326,
                new long[]{3, 1}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("decrementParameters")
    void shouldComputeCorrectDecrementalMetaData(
        long featureVectorIdx,
        long size,
        double expectedLoss,
        long[] expectedClassCounts
    ) {
        var labels = HugeIntArray.of(1, 0, 0, 0, 1);
        var entropyLossLoss = new Entropy(labels, 2);
        var impurityData = entropyLossLoss.groupImpurity(HugeLongArray.of(2, 0, 3, 1, 4), 0, 5);
        entropyLossLoss.decrementalImpurity(featureVectorIdx, impurityData);

        assertThat(impurityData.impurity())
            .isCloseTo(expectedLoss, Offset.offset(0.00001D));
        assertThat(impurityData.classCounts()).containsExactly(expectedClassCounts);
        assertThat(impurityData.groupSize()).isEqualTo(size);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  10,  104",
        " 100,  464"
    })
    void memoryEstimationShouldScaleWithSampleCount(long numberOfTrainingSamples, long expectedBytes) {
        assertThat(Entropy.memoryEstimation(numberOfTrainingSamples))
            .isEqualTo(MemoryRange.of(expectedBytes));
    }
}
