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
package org.neo4j.gds.ml.core.decisiontree;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class GiniIndexTest {

    private static final int[] CLASSES = {0, 1};

    private static Stream<Arguments> giniParameters() {
        return Stream.of(
            Arguments.of(
                new int[]{1, 0, 1, 0},
                new int[][]{new int[]{0, 1}, new int[]{2, 3}},
                new int[]{2, 2},
                0.5D
            ),
            Arguments.of(
                new int[]{0, 0, 1, 1},
                new int[][]{new int[]{0, 1}, new int[]{2, 3}},
                new int[]{2, 2},
                0.0D
            ),
            Arguments.of(
                new int[]{1, 0, 0, 0},
                new int[][]{new int[]{0}, new int[]{1, 2, 3}},
                new int[]{1, 3},
                0.0D
            ),
            Arguments.of(
                new int[]{1, 0, 0, 0},
                new int[][]{new int[]{0, 1}, new int[]{2, 3}},
                new int[]{2, 2},
                0.25D
            ),
            Arguments.of(
                new int[]{1, 0, 0, 0},
                new int[][]{new int[]{0, 1, 0, 0}, new int[]{2, 3, 1, 1}},
                new int[]{2, 2},
                0.25D
            )
        );
    }

    @ParameterizedTest
    @MethodSource("giniParameters")
    public void shouldComputeCorrectLoss(int[] allLabels, int[][] groups, int[] groupSizes, double expectedLoss) {
        var giniIndexLoss = new GiniIndex(CLASSES, allLabels);

        assertThat(giniIndexLoss.splitLoss(groups, groupSizes)).isCloseTo(expectedLoss, Offset.offset(0.00001D));
    }
}
