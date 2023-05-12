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
package org.neo4j.gds.ml.core.optimizer;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.MatrixSum;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.helper.L2Norm;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


class AdamOptimizerTest {

    @Test
    void shouldConverge() {
        Weights<Matrix> weights = new Weights<>(new Matrix(
            new double[]{
                0.1, 0.1, 0.1,
                0.4, 0.3, 0.9,
                0.01, 0.6, 0.5
            },
            3,
            3
        ));

        Variable<Matrix> optimum = new Weights<>(new Matrix(
            new double[]{
                0.11, 0.13, 0.231,
                0.4, 0.3, 0.9,
                0.01, 0.6, 0.15
            },
            3,
            3
        ));

        AdamOptimizer adam = new AdamOptimizer(List.of(weights));
        Variable<Matrix> difference = new MatrixSum(List.of(
            weights, new ConstantScale<>(optimum, -1)
        ));
        var lossFunction = new L2Norm<>(difference);

        double oldLoss = Double.MAX_VALUE;
        while(true) {
            var localCtx = new ComputationContext();
            double newLoss = localCtx.forward(lossFunction).value();
            double d = oldLoss - newLoss;
            if (Math.abs(d) < 1e-7) break;

            oldLoss = newLoss;
            localCtx.backward(lossFunction);

            adam.update(List.of(localCtx.gradient(weights)));
        }

        assertThat(oldLoss).isLessThan(5e-4);
        assertThat(weights.data().cols()).isEqualTo(3);
        assertThat(weights.data().rows()).isEqualTo(3);
        assertThat(weights.data().data())
            .contains(
                new double[]{
                    0.11,
                    0.13,
                    0.231,
                    0.4,
                    0.3,
                    0.9,
                    0.01,
                    0.6,
                    0.15
                },
                Offset.offset(5e-3)
            );
    }

    @Test
    void runDeterministic() {
        var weights = new Weights<>(new Vector(0.1, 0.1, 0.1));
        var expectedOptimum = new Weights<>(new Vector(5.1, 6.1, 7.1));
        var difference = new ElementSum(List.of(weights, new ConstantScale<>(expectedOptimum, -1)));
        var lossFunction = new L2Norm<>(difference);

        AdamOptimizer adam = new AdamOptimizer(List.of(weights), 0.1);

        double oldLoss = Double.MAX_VALUE;
        for (int i = 0; i < 10; i++) {
            var localCtx = new ComputationContext();
            double newLoss = localCtx.forward(lossFunction).value();
            if (Math.abs(oldLoss - newLoss) < 1e-8) break;

            oldLoss = newLoss;
            localCtx.backward(lossFunction);

            adam.update(List.of(localCtx.gradient(weights)));
        }

        assertThat(adam.momentumTerms.get(0)).isEqualTo(new Vector(-0.6513215598999998, -0.6513215598999998, -0.6513215598999998));
        assertThat(adam.velocityTerms.get(0)).isEqualTo(new Vector(0.009955119790251798, 0.009955119790251798, 0.009955119790251798));
        assertThat(weights.data()).isEqualTo(new Vector(1.0999999899999986, 1.0999999899999986, 1.0999999899999986));
    }

    @Test
    void initializedCorrectly() {
        Weights<Matrix> weights = new Weights<>(new Matrix(
            new double[]{
                0.1, 0.1, 0.1,
                0.4, 0.3, 0.9,
                0.01, 0.6, 0.5
            },
            3,
            3
        ));
        var optimizer = new AdamOptimizer(List.of(weights));

        assertThat(optimizer.momentumTerms.get(0)).isNotSameAs(optimizer.velocityTerms.get(0));
    }
}
