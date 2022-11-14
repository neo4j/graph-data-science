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
import org.neo4j.gds.ml.core.helper.TensorTestUtils;
import org.neo4j.gds.ml.core.tensor.Matrix;

import static org.assertj.core.api.Assertions.assertThat;

class CrossEntropyLossTest implements FiniteDifferenceTest {

    @Test
    void shouldApplyCorrectly() {
        var ctx = new ComputationContext();
        var targets = Constant.vector(new double[]{1.0, 2.0, 0.0});
        var predictions = Constant.matrix(
            new double[]{
                0.35, 0.65, 0.0,
                0.45, 0.45, 0.1,
                0.14, 0.66, 0.2
            },
            3, 3
        );

        var loss = new CrossEntropyLoss(predictions, targets, new double[]{0.5, 0.3, 0.2});

        double lossValue = ctx.forward(loss).value();

        var p1 = -Math.log(0.65);
        var p2 = -Math.log(0.1);
        var p3 = -Math.log(0.14);

        var expected = 1.0 / 3 * (0.3*p1 + 0.2*p2 + 0.5*p3);

        assertThat(lossValue).isCloseTo(expected, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeGradientCorrectly() {
        var targets = Constant.vector(new double[]{1.0, 2.0, 0.0});
        var predictions = new Weights<>(
            new Matrix(
                new double[]{
                    0.35, 0.65, 0.0,
                    0.45, 0.45, 0.1,
                    0.14, 0.66, 0.2
                },
                3, 3
            )
        );

        var loss = new CrossEntropyLoss(predictions, targets, new double[]{0.5, 0.3, 0.2});

        finiteDifferenceShouldApproximateGradient(predictions, loss);
    }

    @Test
    void considerSelfGradient() {
        var targets = Constant.vector(new double[]{1.0, 2.0, 0.0});
        var predictions = new Weights<>(
            new Matrix(
                new double[]{
                    0.35, 0.65, 0.0,
                    0.45, 0.45, 0.1,
                    0.14, 0.66, 0.2
                },
                3, 3
            )
        );

        var loss = new CrossEntropyLoss(predictions, targets, new double[]{0.5, 0.3, 0.2});
        var chainedLoss = new Sigmoid<>(loss);

        finiteDifferenceShouldApproximateGradient(predictions, chainedLoss);
    }


    @Test
    void infiniteSmallProbabilities() {
        var predictions = new Weights<>(new Matrix(new double[]{5.277E-321, 5.277E-321}, 1, 2));
        var targets = Constant.vector(new double[]{1});

        var ctx = new ComputationContext();
        var crossEntropyLoss = new CrossEntropyLoss(predictions, targets, new double[]{1,1});

        ctx.forward(crossEntropyLoss);

        ctx.backward(crossEntropyLoss);
        var actualGradient = ctx.gradient(predictions);

        assertThat(actualGradient).matches(TensorTestUtils::containsValidValues);
    }

    @Test
    void classWeightedCEShouldFocusOnHighWeightClass() {
        var labels = Constant.vector(new double[]{1.0, 0.0});
        var weightsBiasedToClass0 = new double[]{0.8, 0.2};

        var predictions = Constant.matrix(new double[]{
            0.4,0.6,
            0.6,0.4
        }, 2, 2);

        var crossEntropyLoss = new CrossEntropyLoss(predictions, labels, weightsBiasedToClass0);
        double crossEntropyLossValue = new ComputationContext().forward(crossEntropyLoss).value();

        var predictionsBiasedToClass0 = Constant.matrix(new double[]{
            0.5,0.5,
            0.7,0.3
        }, 2, 2);
        var weightedCrossEntropyLoss = new CrossEntropyLoss(predictionsBiasedToClass0, labels, weightsBiasedToClass0);
        double weightedCrossEntropyLossValue = new ComputationContext().forward(weightedCrossEntropyLoss).value();

        assertThat(weightedCrossEntropyLossValue).isLessThan(crossEntropyLossValue);
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
