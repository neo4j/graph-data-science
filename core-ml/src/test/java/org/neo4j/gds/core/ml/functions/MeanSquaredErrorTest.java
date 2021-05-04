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
package org.neo4j.gds.core.ml.functions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.FiniteDifferenceTest;
import org.neo4j.gds.core.ml.tensor.Matrix;
import org.neo4j.gds.core.ml.tensor.Scalar;

import java.util.List;

class MeanSquaredErrorTest implements FiniteDifferenceTest {
    @Test
    void testForward() {
        ComputationContext ctx = new ComputationContext();
        Weights<Matrix> a = new Weights<>(new Matrix(new double[]{1, 1, 1, 1, 1, 1}, 6, 1));
        Weights<Matrix> b = new Weights<>(new Matrix(new double[]{3, 2, 2, 2, 2, 1}, 6, 1));

        MeanSquaredError meanSquaredError = new MeanSquaredError(a, b);
        Scalar value = ctx.forward(meanSquaredError);
        Assertions.assertEquals((4 + 1 + 1 + 1 + 1 + 0)/6.0, value.value());
    }

    @Test
    void testGradient() {
        Weights<Matrix> a = new Weights<>(new Matrix(new double[]{1, 1, 1, 1, 1, 1}, 6, 1));
        Weights<Matrix> b = new Weights<>(new Matrix(new double[]{3, 2, 2, 2, 2, 1}, 6, 1));

        finiteDifferenceShouldApproximateGradient(List.of(a, b), new MeanSquaredError(a, b));
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
