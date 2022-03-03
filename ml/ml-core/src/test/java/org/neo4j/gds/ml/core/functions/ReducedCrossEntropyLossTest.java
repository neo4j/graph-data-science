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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;
import java.util.Optional;

import static java.lang.Math.log;
import static org.assertj.core.api.Assertions.assertThat;

class ReducedCrossEntropyLossTest implements FiniteDifferenceTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldApplyCorrectlyReduced(boolean useBias) {
        var weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0, 0.1, 0.54, 0.12, 0.81, 0.7}, 2, 4));
        var features = Constant.matrix(
            new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71, 0.29, -0.52, 0.12, -0.92, 0.6, -0.11},
            3,
            4
        );
        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(features, weights);
        var bias = new Weights<>(new Scalar(0.37));
        var affineVariable = new EWiseAddMatrixScalar(weightedFeatures, bias);
        var predictions = new ReducedSoftmax(useBias ? affineVariable : weightedFeatures);
        var labels = Constant.vector(new double[]{1.0, 0.0, 2.0});

        var loss = new ReducedCrossEntropyLoss(predictions, weights, Optional.empty(), features, labels);
        var ctx = new ComputationContext();

        double lossValue = ctx.forward(loss).value();

        // verify that the predictions are updated in the context
        var expectedPredictions = useBias ? new double[] {
            0.4472428, 0.4327679, 0.1199891,
            0.5096824, 0.3245332, 0.1657843,
            0.3771114, 0.4207929, 0.2020955
        } : new double[] {
            0.4244404, 0.4107036, 0.1648559,
            0.4744641, 0.3021084, 0.2234273,
            0.3458198, 0.3858767, 0.2683033
        };
        assertThat(ctx.data(predictions).data()).containsExactly(
            expectedPredictions,
            Offset.offset(1e-7)
        );

        double expectedLoss = -1.0 / 3.0 * (
            log(expectedPredictions[1]) +
            log(expectedPredictions[3]) +
            log(1 - expectedPredictions[6] - expectedPredictions[7])
        );
        assertThat(lossValue).isCloseTo(expectedLoss, Offset.offset(1e-6));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldApplyCorrectlyStandard(boolean useBias) {
        var weights = new Weights<>(new Matrix(
            new double[]{0.35, 0.41, 1.0, 0.1, 0.54, 0.12, 0.81, 0.7, 0.0, 0.0, 0.0, 0.0},
            3,
            4
        ));
        var features = Constant.matrix(
            new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71, 0.29, -0.52, 0.12, -0.92, 0.6, -0.11},
            3,
            4
        );
        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(features, weights);
        var bias = new Weights<>(new Scalar(0.37));
        var affineVariable = new EWiseAddMatrixScalar(weightedFeatures, bias);
        var predictions = new Softmax(useBias ? affineVariable : weightedFeatures);
        var labels = Constant.vector(new double[]{1.0, 0.0, 2.0});

        var loss = new ReducedCrossEntropyLoss(predictions, weights, Optional.empty(), features, labels);
        var loss2 = new CrossEntropyLoss(predictions, labels);
        var ctx = new ComputationContext();

        double lossValue = ctx.forward(loss).value();
        double lossValue2 = ctx.forward(loss2).value();


        // verify that the predictions are updated in the context
        // note: bias has no effect when scalar and softmax is used
        var expectedPredictions = new double[] {
            0.4244404, 0.4107036, 0.1648559,
            0.4744641, 0.3021084, 0.2234273,
            0.3458198, 0.3858767, 0.2683033
        };
        assertThat(ctx.data(predictions).data()).containsExactly(
            expectedPredictions,
            Offset.offset(1e-7)
        );

        double expectedLoss = -1.0 / 3.0 * (
            log(expectedPredictions[1]) +
            log(expectedPredictions[3]) +
            log(1 - expectedPredictions[6] - expectedPredictions[7])
        );
        assertThat(lossValue).isCloseTo(expectedLoss, Offset.offset(1e-6));
        assertThat(lossValue2).isCloseTo(expectedLoss, Offset.offset(1e-6));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldComputeGradientCorrectlyReduced(boolean useBias) {
        var weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0, 0.1, 0.54, 0.12, 0.81, 0.7}, 2, 4));
        var features = Constant.matrix(
            new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71, 0.29, -0.52, 0.12, -0.92, 0.6, -0.11},
            3,
            4
        );

        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(features, weights);
        var bias = new Weights<>(new Vector(0.37, 0.37));
        var affineVariable = new MatrixVectorSum(weightedFeatures, bias);

        var predictions = new ReducedSoftmax(useBias ? affineVariable : weightedFeatures);
        var labels = Constant.vector(new double[]{1.0, 0.0, 2.0});

        var loss = new ReducedCrossEntropyLoss(
            predictions,
            weights,
            useBias ? Optional.of(bias) : Optional.empty(),
            features,
            labels
        );

        finiteDifferenceShouldApproximateGradient(useBias ? List.of(bias, weights) : List.of(weights), loss);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldComputeGradientCorrectlyStandard(boolean useBias) {
        var weights = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0, 0.1, 0.54, 0.12, 0.81, 0.7, 0.0, 0.0, 0.0, 0.0}, 3, 4));
        var features = Constant.matrix(
            new double[]{0.23, 0.52, 0.62, 0.32, 0.64, 0.71, 0.29, -0.52, 0.12, -0.92, 0.6, -0.11},
            3,
            4
        );

        var weightedFeatures = new MatrixMultiplyWithTransposedSecondOperand(features, weights);
        var bias = new Weights<>(new Vector(0.37, 0.37, 0.37));
        var affineVariable = new MatrixVectorSum(weightedFeatures, bias);

        var predictions = new Softmax(useBias ? affineVariable : weightedFeatures);
        var labels = Constant.vector(new double[]{1.0, 0.0, 2.0});

        var loss = new ReducedCrossEntropyLoss(
            predictions,
            weights,
            useBias ? Optional.of(bias) : Optional.empty(),
            features,
            labels
        );

        finiteDifferenceShouldApproximateGradient(useBias ? List.of(bias, weights) : List.of(weights), loss);
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
