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
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MeanSquareErrorTest implements FiniteDifferenceTest {

    @Test
    void testForward() {
        ComputationContext ctx = new ComputationContext();
        var a = new Weights<>(new Vector(1, 1, 1, 1, 1, 1));
        var b = new Weights<>(new Vector(3, 2, 2, 2, 2, 1));

        var meanSquaredError = new MeanSquareError(a, b);
        assertThat(ctx.forward(meanSquaredError).value()).isEqualTo((4 + 1 + 1 + 1 + 1 + 0) / 6.0);
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.MAX_VALUE, 1e155})
    void applyOnExtremeValues(double extremeValue) {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var predictions = Constant.vector(new double[]{extremeValue, 3, 2, 8});

        var variable = new MeanSquareError(predictions, targets);

        ComputationContext ctx = new ComputationContext();
        assertThat(ctx.forward(variable)).isEqualTo(new Scalar(Double.MAX_VALUE));
    }

    @Test
    void testGradient() {
        var a = new Weights<>(new Vector(1, 1, 1, 1, 1, 1));
        var b = new Weights<>(new Vector(3, 2, 2, 2, 2, 1));

        finiteDifferenceShouldApproximateGradient(List.of(a, b), new MeanSquareError(a, b));
    }

    @Test
    void gradientWithChild() {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var weights = new Weights<>(new Vector(2.5, 0, 2, 8));

        var meanSquareError = new MeanSquareError(weights, targets);

        finiteDifferenceShouldApproximateGradient(weights, new Sigmoid<>(meanSquareError));
    }

    @Test
    void gradientForPerfectPrediction() {
        var targets = Constant.vector(new double[]{30.0});
        var weights = new Weights<>(new Vector(30.0));

        var meanSquareError = new MeanSquareError(weights, targets);

        ComputationContext ctx = new ComputationContext();
        ctx.forward(meanSquareError);
        assertThat(ctx.data(meanSquareError)).isEqualTo(new Scalar(0));
        ctx.backward(meanSquareError);
        assertThat(ctx.gradient(meanSquareError)).isEqualTo(new Scalar(1));
        assertThat(ctx.gradient(weights)).isEqualTo(new Vector(0.0));
    }

    @Test
    void failOnWrongSizedArguments() {
        assertThatThrownBy(() -> new MeanSquareError(Constant.vector(new double[]{1, 1, 1, 1, 1, 1}), Constant.vector(new double[]{1, 1, 1, 1, 1})))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Targets and predictions must be of equal size. Got predictions: Vector(6), targets: Vector(5)");
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
