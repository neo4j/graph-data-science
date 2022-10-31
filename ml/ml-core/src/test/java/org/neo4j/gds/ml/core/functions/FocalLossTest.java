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

import static org.assertj.core.api.Assertions.assertThat;

class FocalLossTest implements FiniteDifferenceTest {

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

        var loss = new FocalLoss(predictions, targets, 5);

        finiteDifferenceShouldApproximateGradient(predictions, loss);
    }

    @Test
    void focalLossShouldDiscourageLowConfidencePrediction() {
        var labels = Constant.vector(new double[]{1.0, 0.0});

        var predictions = Constant.matrix(new double[]{
            0.01,0.99,
            0.99,0.01
        }, 2, 2);

        var crossEntropyLoss = new CrossEntropyLoss(predictions, labels);
        double crossEntropyLossValue = new ComputationContext().forward(crossEntropyLoss).value();

        var loss = new FocalLoss(predictions, labels, 0);
        double lossValue = new ComputationContext().forward(loss).value();

        assertThat(crossEntropyLossValue).isEqualTo(lossValue);

        var focalLoss = new FocalLoss(predictions, labels, 5);
        double focalLossValue = new ComputationContext().forward(focalLoss).value();

        assertThat(lossValue).isGreaterThan(focalLossValue);

        var badPredictions = Constant.matrix(new double[]{
            0.01,0.99,
            0.01,0.99
        }, 2, 2);
        var focalLossForBadPredictions = new FocalLoss(badPredictions, labels, 5);
        double focalLossValueForBadPredictions = new ComputationContext().forward(focalLossForBadPredictions).value();

        assertThat(focalLossValue).isLessThan(focalLossValueForBadPredictions);
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
