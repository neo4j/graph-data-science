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

import org.neo4j.gds.ml.core.AbstractVariable;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

public class RootMeanSquareError extends AbstractVariable<Scalar> {

    private final Variable<Matrix> predictionsVar;
    private final Variable<Vector> targetsVar;

    public RootMeanSquareError(
        Variable<Matrix> predictions,
        Variable<Vector> targets
    ) {
        super(List.of(predictions, targets), Dimensions.scalar());

        assert Dimensions.isVector(predictions.dimensions()) : "Predictions need to be a vector";
        assert Dimensions.totalSize(predictions.dimensions()) > 0;
        assert Dimensions.totalSize(predictions.dimensions()) == Dimensions.totalSize(targets.dimensions()) : "Predictions and targets need to have the same total size";

        this.predictionsVar = predictions;
        this.targetsVar = targets;

    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Matrix predictions = ctx.data(predictionsVar);
        Vector targets = ctx.data(targetsVar);

        double squaredErrorSum = 0D;
        for (int idx = 0; idx < targets.length(); idx++) {
            double error = predictions.dataAt(idx) - targets.dataAt(idx);
            squaredErrorSum += error * error;
        }

        if (!Double.isFinite(squaredErrorSum)) {
            return new Scalar(Double.MAX_VALUE);
        }

        return new Scalar(Math.sqrt(squaredErrorSum / targets.length()));
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == predictionsVar) {
            Matrix predictions = ctx.data(predictionsVar);
            var numberOfExamples = ctx.data(targetsVar).length();
            var rootOfSumOfSquareErrorOverN = ctx.data(this);

            var parentGradient = ctx.data(parent).createWithSameDimensions();
            if (Double.compare(rootOfSumOfSquareErrorOverN.value(), 0) == 0) {
                return parentGradient;
            }

            var denominator = rootOfSumOfSquareErrorOverN.scalarMultiply(numberOfExamples).value();
            double selfGradient = ctx.gradient(this).value();
            var scale = selfGradient / denominator;

            Vector targets = ctx.data(targetsVar);
            for (int idx = 0; idx < numberOfExamples; idx++) {
                double error = predictions.dataAt(idx) - targets.dataAt(idx);
                parentGradient.setDataAt(idx, error * scale);
            }

            return parentGradient;
        }
        throw new IllegalStateException(
            "The gradient should only be computed for the prediction parent, but got " + parent.render()
        );
    }
}
