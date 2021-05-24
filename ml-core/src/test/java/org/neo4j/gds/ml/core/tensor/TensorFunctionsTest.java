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
package org.neo4j.gds.ml.core.tensor;


import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TensorFunctionsTest {

    @ParameterizedTest(name = "{2}")
    @MethodSource("batchedTensors")
    void averageTensorsTest(
        List<? extends List<? extends Tensor<?>>> batchedTensors,
        List<? extends List<? extends Tensor<?>>> expectedAveragedTensors,
        String description
    ) {
        var averageGradients = TensorFunctions.averageTensors(batchedTensors);

        assertThat(averageGradients)
            .isSameAs(batchedTensors.get(0)) // make sure we reuse the first entry in the list
            .isEqualTo(expectedAveragedTensors);
    }

    private static Stream<Arguments> batchedTensors() {
        return Stream.of(
            Arguments.of(
                List.of(
                    List.of(new Scalar(10d), new Scalar(14d)),
                    List.of(new Scalar(2d), new Scalar(3d)),
                    List.of(new Scalar(6d), new Scalar(7d))
                ),
                List.of(new Scalar(6d), new Scalar(8d)),
                "Scalar Average"
            ),
            Arguments.of(
                List.of(
                    List.of(new Vector(10d, 14d)),
                    List.of(new Vector(2d, 3d)),
                    List.of(new Vector(6d, 7d))
                ),
                List.of(new Vector(6d, 8d)),
                "Vector Average"
            ),
            Arguments.of(
                List.of(
                    List.of(new Matrix(new double[]{10d, 14d}, 1, 2)),
                    List.of(new Matrix(new double[]{2d, 3d}, 1, 2)),
                    List.of(new Matrix(new double[]{6d, 7d}, 1, 2))
                ),
                List.of(new Matrix(new double[]{6d, 8d}, 1, 2)),
                "Matrix Average"
            )
        );
    }
}
