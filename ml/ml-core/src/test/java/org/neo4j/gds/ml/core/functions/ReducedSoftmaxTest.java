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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.tensor.Matrix;

import static org.assertj.core.api.Assertions.assertThat;

class ReducedSoftmaxTest implements FiniteDifferenceTest {

    @Test
    void shouldNotOverflowIntoInfinity() {
        var ctx = new ComputationContext();
        var parent = Constant.matrix(new double[]{-745.1, -745.15, 709.7, 709.8}, 4, 1);
        ctx.forward(parent);

        var softmax = new ReducedSoftmax(parent);

        var result = softmax.apply(ctx);

        var expected = new Matrix(new double[]{
            0.0, 1.0,
            0.0, 1.0,
            1.0, 0.0,
            1.0, 0.0
        }, 4, 2);

        assertThat(result).matches(matrix -> matrix.equals(expected, 1e-8));
    }

    @Test
    void shouldApply() {
        var ctx = new ComputationContext();
        var matrixConstant = Constant.matrix(
            new double[]{
                1.7, 2.2, -0.4, 2.3, 4.3,
                0, 0, 0, 0, 0,
                1.7, 2.2, -0.4, 2.3, 4.3,
            },
            3, 5
        );
        var softmax = new ReducedSoftmax(matrixConstant);

        var result = ctx.forward(softmax);

        var expected = new Matrix(new double[]{
            0.054825408857653565, 0.09039181775844467, 0.006713723746217652, 0.0998984082186269, 0.7381549425213091, 0.01001569889774817,
            1.0 / 6, 1.0 / 6, 1.0 / 6, 1.0 / 6, 1.0 / 6, 1.0/6,
            0.054825408857653565, 0.09039181775844467, 0.006713723746217652, 0.0998984082186269, 0.7381549425213091, 0.01001569889774817
        }, 3, 6);
        assertThat(result).matches(matrix -> matrix.equals(expected, 1e-8));
        for (int row = 0; row < result.rows(); row++) {
            var sum = 0.0;
            for (int col = 0; col < result.cols(); col++) {
                sum += result.dataAt(row, col);
            }
            assertThat(sum).isEqualTo(1.0, Offset.offset(1e-9));
        }

    }

    @Test
    void computesGradientCorrectly() {
        var matrixConstant = Constant.matrix(
            new double[]{
                13.37, 13.37, 13.37, 13.37, 13.37, 13.37
            },
            1, 6
        );
        var weights = new Weights<>(
            new Matrix(
                new double[]{
                    0.6, 1.1, -1.5, 1.2, 3.2
                },
                1, 5
            )
        );
        var softmax = new ReducedSoftmax(weights);
        var loss = new MeanSquareError(softmax, matrixConstant);

        finiteDifferenceShouldApproximateGradient(weights, loss);
    }
}
