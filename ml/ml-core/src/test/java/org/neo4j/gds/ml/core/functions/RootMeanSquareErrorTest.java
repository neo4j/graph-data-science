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
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;

import static org.assertj.core.api.Assertions.assertThat;

class RootMeanSquareErrorTest implements FiniteDifferenceTest {

    @Override
    public double epsilon() {
        return 1e-9;
    }

    @Test
    void apply() {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var predictions = Constant.matrix(new double[]{2.5, 0, 2, 8}, 1, 4);

        var variable = new RootMeanSquareError(predictions, targets);

        ComputationContext ctx = new ComputationContext();
        assertThat(ctx.forward(variable)).isEqualTo(new Scalar(Math.sqrt(0.375)));
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.MAX_VALUE, 1e155})
    void applyOnExtremeValues(double extremeValue) {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var predictions = Constant.matrix(new double[]{extremeValue, 3, 2, 8}, 1, 4);

        var variable = new RootMeanSquareError(predictions, targets);

        ComputationContext ctx = new ComputationContext();
        assertThat(ctx.forward(variable)).isEqualTo(new Scalar(Double.MAX_VALUE));
    }

    @Test
    void gradient() {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var weights = new Weights<>(new Matrix(new double[]{2.5, 0, 2, 8}, 1, 4));

        var rmse = new RootMeanSquareError(weights, targets);

        finiteDifferenceShouldApproximateGradient(weights, rmse);
    }

    @Test
    void gradientWithChild() {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var weights = new Weights<>(new Matrix(new double[]{2.5, 0, 2, 8}, 1, 4));

        var rmse = new RootMeanSquareError(weights, targets);

        finiteDifferenceShouldApproximateGradient(weights, new Sigmoid<>(rmse));
    }

    @Test
    void gradientForPerfectPrediction() {
        var targets = Constant.vector(new double[]{30.0});
        var weights = new Weights<>(new Matrix(new double[]{30.0}, 1, 1));

        var rmse = new RootMeanSquareError(weights, targets);

        ComputationContext ctx = new ComputationContext();
        ctx.forward(rmse);
        assertThat(ctx.data(rmse)).isEqualTo(new Scalar(0));
        ctx.backward(rmse);
        assertThat(ctx.gradient(rmse)).isEqualTo(new Scalar(1));
        assertThat(ctx.gradient(weights)).isEqualTo(new Matrix(new double[]{0.0}, 1, 1));
    }
}
