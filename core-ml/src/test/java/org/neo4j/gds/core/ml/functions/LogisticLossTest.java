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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.FiniteDifferenceTest;
import org.neo4j.gds.core.ml.tensor.Matrix;

import static java.lang.Math.log;
import static org.assertj.core.api.Assertions.assertThat;

class LogisticLossTest implements FiniteDifferenceTest {

    @Test
    void shouldApplyCorrectly() {
        MatrixConstant targets = new MatrixConstant(new double[]{1.0, 0.0}, 2, 1);
        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0}, 1, 3));
        MatrixConstant features = new MatrixConstant(new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71}, 2, 3);
        Sigmoid<Matrix> predictions = new Sigmoid<>(new MatrixMultiplyWithTransposedSecondOperand(features, weights));
        var loss = new LogisticLoss(weights, predictions, features, targets);
        ComputationContext ctx = new ComputationContext();
        double lossValue = ctx.forward(loss).value();
        double expectedValue = -1.0/2.0 * (1.0*log(0.7137567) + 0 + 0 + log(1-0.74732574));
        assertThat(ctx.data(predictions).data()).containsExactly(new double[]{0.7137567 , 0.74732574}, Offset.offset(1e-6));
        assertThat(lossValue).isCloseTo(expectedValue, Offset.offset(1e-8));
    }

    @Test
    void logisticLossApproximatesGradient() {
        MatrixConstant targets = new MatrixConstant(new double[]{1.0, 0.0}, 2, 1);
        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0}, 1, 3));
        MatrixConstant features = new MatrixConstant(new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71}, 2, 3);
        Sigmoid<Matrix> predictions = new Sigmoid<>(new MatrixMultiplyWithTransposedSecondOperand(features, weights));

        var loss = new LogisticLoss(weights, predictions, features, targets);
        finiteDifferenceShouldApproximateGradient(weights, loss);
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }

}
