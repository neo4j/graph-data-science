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
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EWiseAddMatrixScalarTest implements FiniteDifferenceTest {

    @Test
    void adds() {
        var scalarVariable = Constant.scalar(1.4);
        var matrixVariable = Constant.matrix(new double[]{2, -2, 3.2, 42.42, 1337, -99.99}, 2, 3);
        var addVar = new EWiseAddMatrixScalar(matrixVariable, scalarVariable);

        assertThat(addVar.dimensions()).containsExactly(matrixVariable.dimensions());

        Matrix expected = new Matrix(new double[]{3.4, -0.6, 4.6, 43.82, 1338.4, -98.59}, 2, 3);
        assertThat(new ComputationContext().forward(addVar)).matches(actual -> actual.equals(expected, 1e-12));
    }

    @Test
    void shouldApproximateGradient() {
        var scalarVariable = Constant.scalar(1.4);
        var matrixVariable = Constant.matrix(new double[]{2, -2, 3.2, 42.42, 1337, -99.99}, 2, 3);
        var addVar = new EWiseAddMatrixScalar(matrixVariable, scalarVariable);
        var weights = new Weights<>(new Scalar(1.4));

        finiteDifferenceShouldApproximateGradient(weights, new ElementSum(List.of(addVar, weights)));
    }

}
