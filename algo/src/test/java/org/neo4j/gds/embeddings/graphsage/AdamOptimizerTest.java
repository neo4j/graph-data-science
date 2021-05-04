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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.functions.ConstantScale;
import org.neo4j.gds.core.ml.functions.MatrixSum;
import org.neo4j.gds.core.ml.functions.Weights;
import org.neo4j.gds.core.ml.helper.L2Norm;
import org.neo4j.gds.core.ml.tensor.Matrix;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertTrue(oldLoss < 1e-4, "oldLoss was : " + oldLoss);
    }

}
