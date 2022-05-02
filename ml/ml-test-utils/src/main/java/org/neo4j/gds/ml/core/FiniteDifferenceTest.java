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
package org.neo4j.gds.ml.core;

import org.assertj.core.data.Offset;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;

import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.ml.core.Dimensions.totalSize;

public interface FiniteDifferenceTest {

    String FAIL_MESSAGE = "AutoGrad of %f and FiniteDifference gradients of %f differs for coordinate %s more than the tolerance.";

    default double tolerance() {
        return 1E-5;
    }

    default double epsilon() {
        return 1E-4;
    }

    default void finiteDifferenceShouldApproximateGradient(Weights<?> weightVariable, Variable<Scalar> loss) {
        finiteDifferenceShouldApproximateGradient(List.of(weightVariable), loss);
    }

    default void finiteDifferenceShouldApproximateGradient(List<Weights<?>> weightVariables, Variable<Scalar> loss) {
        for (Weights<?> variable : weightVariables) {
            for (int tensorIndex = 0; tensorIndex < totalSize(variable.dimensions()); tensorIndex++) {
                ComputationContext ctx = new ComputationContext();
                double forwardLoss = ctx.forward(loss).value();
                ctx.backward(loss);
                var autoGradient = ctx.gradient(variable).dataAt(tensorIndex);

                // perturb data
                variable.data().addDataAt(tensorIndex, epsilon());

                ComputationContext ctx2 = new ComputationContext();
                double forwardLossOnPerturbedData = ctx2.forward(loss).value();

                double finiteDifferenceGrad = (forwardLossOnPerturbedData - forwardLoss) / epsilon();

                assertThat(finiteDifferenceGrad)
                    .isNotNaN()
                    .withFailMessage(FAIL_MESSAGE, autoGradient, finiteDifferenceGrad, tensorIndex)
                    .isEqualTo(autoGradient, Offset.offset(tolerance()));
            }
        }
    }

}
