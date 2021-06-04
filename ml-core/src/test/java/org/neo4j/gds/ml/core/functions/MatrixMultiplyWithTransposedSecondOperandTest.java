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
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.helper.L2Norm;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class MatrixMultiplyWithTransposedSecondOperandTest extends ComputationGraphBaseTest implements FiniteDifferenceTest {

    @Override
    public double epsilon() {
        return 1e-6;
    }

    @Test
    void testMultiply() {
        double[] m1 = {
            1, 2, 3,
            4, 5, 6
        };

        double[] m2 = {
            1, 4, 6,
            2.1, 5, -1
        };

        var expected = new Matrix(new double[]{
            27,  9.1,
            60, 27.4
        }, 2, 2);

        var A = Constant.matrix(m1, 2, 3);
        var B = Constant.matrix(m2, 2, 3);

        Variable<Matrix> product = new MatrixMultiplyWithTransposedSecondOperand(A, B);

        assertThat(ctx.forward(product)).isEqualTo(expected);
    }

    @Test
    void shouldApproximateGradient() {
        double[] m1 = {
            1, 2, 3,
            4, 5, 6
        };

        double[] m2 = {
            1, 4, 6,
            2.1, 5, -1
        };

        Weights<Matrix> A = new Weights<>(new Matrix(m1, 2, 3));
        Weights<Matrix> B = new Weights<>(new Matrix(m2, 2, 3));

        finiteDifferenceShouldApproximateGradient(List.of(A, B), new L2Norm(new MatrixMultiplyWithTransposedSecondOperand(A, B)));
    }

    @Test
    void shouldDisallowMultiplication() {
        double[] m1 = {
            1, 2, 3,
            4, 5, 6
        };

        double[] m2 = {
            1, 4,
            6, 2.1,
            5, -1
        };
        Weights<Matrix> A = new Weights<>(new Matrix(m1, 2, 3));
        Weights<Matrix> B = new Weights<>(new Matrix(m2, 3, 2));

        AssertionError assertionError = assertThrows(
            AssertionError.class,
            () -> new MatrixMultiplyWithTransposedSecondOperand(A, B)
        );

        assertEquals(
            formatWithLocale(
                "Cannot multiply matrix having dimensions (%d, %d) with transposed matrix of dimensions (%d, %d)",
                A.dimension(1), A.dimension(0),
                B.dimension(0), B.dimension(1)
            ),
            assertionError.getMessage()
        );
    }

}
