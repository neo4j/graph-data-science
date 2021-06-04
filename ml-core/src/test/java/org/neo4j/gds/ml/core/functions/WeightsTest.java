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
package org.neo4j.gds.ml.core.functions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WeightsTest {

    @Test
    void inAndOutInT() {
        Matrix in = Matrix.fill(1, 2, 3);
        Matrix out = new Weights<>(in).data();

        assertEquals(in, out);
    }

    private static Stream<Arguments> renderSources() {
        return Stream.of(
            Arguments.of(new Scalar(0D), "Weights: Scalar: [0.0], requireGradient: true" + System.lineSeparator()),
            Arguments.of(new Vector(new double[]{0, 2, 4}), "Weights: Vector(3): [0.0, 2.0, 4.0], requireGradient: true" + System.lineSeparator()),
            Arguments.of(new Matrix(new double[]{0, 2, 4, 2}, 2, 2), "Weights: Matrix(2, 2): [0.0, 2.0, 4.0, 2.0], requireGradient: true" + System.lineSeparator())
        );
    }

    @ParameterizedTest
    @MethodSource("renderSources")
    void render(Tensor data, String expected) {
        assertThat(new Weights<>(data).render()).isEqualTo(expected);
    }
}
