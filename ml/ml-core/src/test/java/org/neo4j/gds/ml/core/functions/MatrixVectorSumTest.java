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
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class MatrixVectorSumTest extends ComputationGraphBaseTest implements FiniteDifferenceTest {

    @Test
    void shouldBroadcastSum() {
        var matrix = Constant.matrix(new double[]{1, 2, 3, 4, 5, 7}, 2, 3);
        Constant<Vector> vector = Constant.vector(new double[]{1, 2, 1});

        Variable<Matrix> broadcastSum = new MatrixVectorSum(matrix, vector);

        var expected = new Matrix(new double[]{2, 4, 4, 5, 7, 8}, 2, 3);
        assertThat(ctx.forward(broadcastSum))
            .isEqualTo(expected);
    }


    @Test
    void shouldComputeVectorGradient() {
        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{1, 2, 3, 4, 5, 7}, 2, 3));
        Weights<Vector> vector = new Weights<>(Vector.create(1, 3));

        var matrixVectorSum = new MatrixVectorSum(weights, vector);
        Variable<Scalar> broadcastSum = new ElementSum(List.of(new Sigmoid<>(matrixVectorSum)));

        ComputationContext ctx = new ComputationContext();
        ctx.forward(broadcastSum);
        ctx.backward(broadcastSum);

        assertThat(ctx.gradient(weights)).isEqualTo(ctx.gradient(matrixVectorSum));
        Vector expectedGradient = new Vector(0.11164164, 0.04764316, 0.01799794);
        assertThat(ctx.gradient(vector)).matches(actual -> actual.equals(expectedGradient, 1e-8));
    }

    @Test
    void shouldApproximateGradient() {
        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{1, 2, 3, 4, 5, 7}, 2, 3));
        Weights<Vector> vector = new Weights<>(Vector.create(1, 3));

        Variable<Scalar> broadcastSum = new ElementSum(List.of(new MatrixVectorSum(weights, vector)));

        finiteDifferenceShouldApproximateGradient(List.of(weights, vector), broadcastSum);
    }

    @ParameterizedTest (name = "Vector length: {1}; matrix columns: 3")
    @MethodSource("invalidVectors")
    void assertionErrorWhenVectorHasDifferentLengthThanMatrixColumns(Variable<Vector> vector, int vectorLength) {
        var matrix = Constant.matrix(new double[]{1, 2, 3, 4, 5, 7}, 2, 3);

        AssertionError assertionError = assertThrows(AssertionError.class, () -> new MatrixVectorSum(matrix, vector));

        assertEquals(
            formatWithLocale("Cannot broadcast vector with length %d to a matrix with %d columns", vector.dimension(0), 3),
            assertionError.getMessage()
        );
    }

    static Stream<Arguments> invalidVectors() {
        return Stream.of(
            Arguments.of(Constant.vector(new double[]{ 1 }), 1),
            Arguments.of(Constant.vector(new double[]{ 1, 2 }), 2),
            Arguments.of(Constant.vector(new double[]{ 1, 2, 3, 4 }), 4),
            Arguments.of(Constant.vector(new double[]{ 1, 2, 3, 4, 5, 6, 7 }), 7)
        );
    }

}
