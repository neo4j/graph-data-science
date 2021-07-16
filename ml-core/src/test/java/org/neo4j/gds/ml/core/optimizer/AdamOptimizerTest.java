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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.MatrixSum;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.helper.L2Norm;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

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

        Variable<Matrix> expectedOptimum = new Weights<>(new Matrix(
            new double[]{
                0.11, 0.13, 0.231,
                0.4, 0.3, 0.9,
                0.01, 0.6, 0.15
            },
            3,
            3
        ));

        ComputationContext ctx = new ComputationContext();
        // need to populate context with data from weights
        ctx.forward(weights);
        AdamOptimizer adam = new AdamOptimizer(List.of(weights));

        double oldLoss = Double.MAX_VALUE;
        while(true) {
            Variable<Matrix> difference = new MatrixSum(List.of(
                weights, new ConstantScale<>(expectedOptimum, -1)
            ));
            var lossFunction = new L2Norm(difference);

            double newLoss = ctx.forward(lossFunction).value();
            double d = oldLoss - newLoss;
            if (Math.abs(d) < 1e-8) break;

            oldLoss = newLoss;
            ctx.backward(lossFunction);

            adam.update(ctx);
        }

        assertThat(oldLoss).isLessThan(1e-4);
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
