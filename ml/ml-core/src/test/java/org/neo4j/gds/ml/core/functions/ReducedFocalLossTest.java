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

import java.util.List;

import static java.lang.Math.log;
import static org.assertj.core.api.Assertions.assertThat;

class ReducedFocalLossTest implements FiniteDifferenceTest {

    @Test
    void shouldComputeGradientCorrectlyReduced() {
        var weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0, 0.1}, 1, 4));
        var features = Constant.matrix(
            new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71, 0.29, -0.52, 0.12, -0.92, 0.6, -0.11},
            3,
            4
        );

        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(features, weights);
        var bias = Weights.ofVector(0.37);
        var affineVariable = new MatrixVectorSum(weightedFeatures, bias);

        var predictions = new ReducedSoftmax(affineVariable);
        var labels = Constant.vector(new double[]{1.0, 0.0, 0.0});

        var loss = new ReducedFocalLoss(
            predictions,
            weights,
            bias,
            features,
            labels,
            50
        );

        var chainedLoss = new Sigmoid<>(loss);

        finiteDifferenceShouldApproximateGradient(List.of(bias, weights), loss);
        finiteDifferenceShouldApproximateGradient(List.of(bias, weights), chainedLoss);
    }

    @Test
    void shouldApplyCorrectlyReduced() {
        var weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0, 0.1, 0.54, 0.12, 0.81, 0.7}, 2, 4));
        var bias = Weights.ofVector(0.37, 0.37);
        var features = Constant.matrix(
            new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71, 0.29, -0.52, 0.12, -0.92, 0.6, -0.11},
            3,
            4
        );
        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(features, weights);
        var affineVariable = new MatrixVectorSum(weightedFeatures, bias);
        var predictions = new ReducedSoftmax(affineVariable);
        var labels = Constant.vector(new double[]{1.0, 0.0, 2.0});

        var loss = new ReducedFocalLoss(predictions, weights, bias, features, labels, 0.5);
        var ctx = new ComputationContext();

        double lossValue = ctx.forward(loss).value();

        // verify that the predictions are updated in the context
        var expectedPredictions = new double[] {
            0.4472428, 0.4327679, 0.1199891,
            0.5096824, 0.3245332, 0.1657843,
            0.3771114, 0.4207929, 0.2020955
        };
        assertThat(ctx.data(predictions).data()).containsExactly(
            expectedPredictions,
            Offset.offset(1e-7)
        );

        double expectedLoss = -1.0 / 3.0 * (
            Math.pow(1-expectedPredictions[1], 0.5) * log(expectedPredictions[1]) +
            Math.pow(1-expectedPredictions[3], 0.5) * log(expectedPredictions[3]) +
            Math.pow(expectedPredictions[6]+expectedPredictions[7], 0.5) * log(1 - expectedPredictions[6] - expectedPredictions[7])
        );
        assertThat(lossValue).isCloseTo(expectedLoss, Offset.offset(1e-6));
    }

    @Test
    void focalLossShouldDiscourageLowConfidencePrediction() {
        var weights = new Weights<>(new Matrix(new double[]{0.9}, 1, 1));
        var bias = Weights.ofVector(0.9);
        var features = Constant.matrix(
            new double[]{0.23, 0.52},
            2,
            1
        );
        var labels = Constant.vector(new double[]{1.0, 0.0});

        var predictions = Constant.matrix(new double[]{
            0.01,0.99,
            0.99,0.01
        }, 2, 2);

        var crossEntropyLoss = new ReducedCrossEntropyLoss(predictions, weights, bias, features, labels);
        double crossEntropyLossValue = new ComputationContext().forward(crossEntropyLoss).value();

        var loss = new ReducedFocalLoss(predictions, weights, bias, features, labels, 0);
        double lossValue = new ComputationContext().forward(loss).value();

        assertThat(crossEntropyLossValue).isEqualTo(lossValue);

        var focalLoss = new ReducedFocalLoss(predictions, weights, bias, features, labels, 5);
        double focalLossValue = new ComputationContext().forward(focalLoss).value();

        assertThat(lossValue).isGreaterThan(focalLossValue);

        var badPredictions = Constant.matrix(new double[]{
            0.01,0.99,
            0.01,0.99
        }, 2, 2);
        var focalLossForBadPredictions = new ReducedFocalLoss(badPredictions, weights, bias, features, labels, 5);
        double focalLossValueForBadPredictions = new ComputationContext().forward(focalLossForBadPredictions).value();

        assertThat(focalLossValue).isLessThan(focalLossValueForBadPredictions);
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
