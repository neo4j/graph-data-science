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
package org.neo4j.gds.core.ml.functions;

import org.neo4j.gds.core.ml.AbstractVariable;
import org.neo4j.gds.core.ml.ComputationContext;
import org.neo4j.gds.core.ml.Dimensions;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.tensor.Matrix;
import org.neo4j.gds.core.ml.tensor.Scalar;
import org.neo4j.gds.core.ml.tensor.Tensor;

import java.util.List;

import static java.lang.Math.log;

/**
   This variable represents the composition of the logistic regression model's prediction function
   and cross-entropy loss. This therefore represents a function from weights, features and targets
   to a scalar loss value. Compared to using CrossEntropyLoss variable, composed with predictions from
   the model, this variable does not register the predictions as a parent in the computation graph.
   Rather, the gradient method directly computes the loss gradient for the weights and circumvents
   the loss gradient for the predictions variable.
   Another advantage of using LogisticLoss is that the expression for the gradient for the weights is
   much simpler than the gradient obtained by back-propagating through the predictions variable.
   In a compact form this gradient expression is just '(predictions - targets) * features'.
 */
public class LogisticLoss extends AbstractVariable<Scalar> {
    // 1 x d
    private final Variable<Matrix> weights;
    // n x 1
    private final Variable<Matrix> predictions;
    // n x d
    private final Variable<Matrix> features;
    // n x 1
    private final Variable<Matrix> targets;

    public LogisticLoss(
        Variable<Matrix> weights,
        Variable<Matrix> predictions,
        Variable<Matrix> features,
        Variable<Matrix> targets
    ) {
        super(List.of(weights, features, targets), Dimensions.scalar());
        this.weights = weights;
        this.predictions = predictions;
        this.features = features;
        this.targets = targets;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        ctx.forward(predictions);
        Matrix predTensor = ctx.data(predictions);
        Matrix targetTensor = ctx.data(targets);

        double result = 0;
        for(int row = 0; row < predTensor.rows(); row++) {
            double v1 = targetTensor.dataAt(row) * log(predTensor.dataAt(row));
            double v2 = (1.0 - targetTensor.dataAt(row)) * log(1.0 - predTensor.dataAt(row));
            if (predTensor.dataAt(row) == 0) {
                result += v2;
            }
            else if (predTensor.dataAt(row) == 1.0) {
                result += v1;
            } else {
                result += v1 + v2;
            }
        }

        return new Scalar(-result/predTensor.rows());
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == weights) {

            ctx.forward(predictions);
            Matrix predTensor = ctx.data(predictions);
            Matrix targetTensor = ctx.data(targets);
            Matrix weightsTensor = ctx.data(weights);
            Matrix featuresTensor = ctx.data(features);
            Matrix gradient = weightsTensor.zeros();
            int cols = weightsTensor.cols();
            int nodes = predTensor.rows();
            for (int node = 0; node < nodes; node++) {
                double errorPerNode = (predTensor.dataAt(node) - targetTensor.dataAt(node)) / nodes;
                for (int dim = 0; dim < cols; dim++) {
                    gradient.addDataAt(dim, errorPerNode * featuresTensor.dataAt(node * cols + dim));
                }
            }
            return gradient;
        } else {
            // assume other variables do not require gradient
            return ctx.data(parent).zeros();
        }
    }
}
