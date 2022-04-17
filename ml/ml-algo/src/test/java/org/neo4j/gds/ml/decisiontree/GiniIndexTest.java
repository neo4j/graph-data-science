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
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GiniIndexTest {

    private static final LocalIdMap CLASS_MAPPING = LocalIdMap.of(5, 1);

    private static Stream<Arguments> giniParameters() {
        return Stream.of(
            Arguments.of(
                HugeLongArray.of(1, 5, 1, 5),
                HugeLongArray.of(0, 1),
                HugeLongArray.of(0, 0, 2, 3),
                2,
                0.5D
            ),
            Arguments.of(
                HugeLongArray.of(5, 5, 1, 1),
                HugeLongArray.of(0, 1),
                HugeLongArray.of(0, 0, 2, 3),
                2,
                0.0D
            ),
            Arguments.of(
                HugeLongArray.of(1, 5, 5, 5),
                HugeLongArray.of(0),
                HugeLongArray.of(0, 1, 2, 3),
                1,
                0.0D
            ),
            Arguments.of(
                HugeLongArray.of(1, 5, 5, 5),
                HugeLongArray.of(0, 1),
                HugeLongArray.of(0, 0, 2, 3),
                2,
                0.25D
            ),
            Arguments.of(
                HugeLongArray.of(1, 5, 5, 5),
                HugeLongArray.of(0, 1, 0, 0),
                HugeLongArray.of(1, 1, 2, 3),
                2,
                0.25D
            )
        );
    }

    @ParameterizedTest
    @MethodSource("giniParameters")
    void shouldComputeCorrectLoss(
        HugeLongArray labels,
        HugeLongArray leftGroup,
        HugeLongArray rightGroup,
        long leftGroupSize,
        double expectedLoss
    ) {
        var giniIndexLoss = GiniIndex.fromOriginalLabels(labels, CLASS_MAPPING);

        assertThat(giniIndexLoss.splitLoss(leftGroup, rightGroup, leftGroupSize))
            .isCloseTo(expectedLoss, Offset.offset(0.00001D));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  10,  104",
        " 100,  464"
    })
    void memoryEstimationShouldScaleWithSampleCount(long numberOfTrainingSamples, long expectedBytes) {
        assertThat(GiniIndex.memoryEstimation(numberOfTrainingSamples))
            .isEqualTo(MemoryRange.of(expectedBytes));
    }
}
