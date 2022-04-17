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
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SplitMeanSquareErrorTest {

    private static Stream<Arguments> MSEParameters() {
        return Stream.of(
            Arguments.of(
                HugeDoubleArray.of(1, 5, 1, 5),
                HugeLongArray.of(0, 1),
                HugeLongArray.of(0, 0, 2, 3),
                2,
                4.0 + 4.0
            ),
            Arguments.of(
                HugeDoubleArray.of(5, 5, 1, 1),
                HugeLongArray.of(0, 1),
                HugeLongArray.of(0, 0, 2, 3),
                2,
                0.0 + 0.0
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 5, 5, 5),
                HugeLongArray.of(0),
                HugeLongArray.of(0, 1, 2, 3),
                1,
                0.0 + 0.0
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 5, 5, 5),
                HugeLongArray.of(0, 1),
                HugeLongArray.of(0, 0, 2, 3),
                2,
                4.0 + 0.0
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 5, 5, 5),
                HugeLongArray.of(0, 1, 0, 1337),
                HugeLongArray.of(42, 1, 2, 3),
                2,
                4.0 + 0.0
            ),
            Arguments.of(
                HugeDoubleArray.of(1, 10, 100, 1000),
                HugeLongArray.of(),
                HugeLongArray.of(0, 1, 2, 3),
                0,
                0.0 + 175380.19
            )
        );
    }

    @ParameterizedTest
    @MethodSource("MSEParameters")
    void shouldComputeCorrectLoss(
        HugeDoubleArray targets,
        HugeLongArray leftGroup,
        HugeLongArray rightGroup,
        long leftGroupSize,
        double expectedLoss
    ) {
        var mse = new SplitMeanSquareError(targets);

        assertThat(mse.splitLoss(leftGroup, rightGroup, leftGroupSize))
            .isCloseTo(expectedLoss, Offset.offset(0.01D));
    }

    @Test
    void shouldEstimateMemory() {
        assertThat(SplitMeanSquareError.memoryEstimation())
            .isEqualTo(MemoryRange.of(16));
    }
}
