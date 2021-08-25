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
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

import static java.lang.Math.log;
import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.isVector;
import static org.neo4j.gds.ml.core.Dimensions.scalar;
import static org.neo4j.gds.ml.core.Dimensions.totalSize;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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
public class LogisticLossWithBias extends AbstractVariable<Scalar> {
    // 1 x d
    private final Variable<Matrix> weights;
    private final Variable<Scalar> bias;
    // n x 1
    private final Variable<Matrix> predictions;
    // n x d
    private final Variable<Matrix> features;
    // n x 1
    private final Variable<Vector> targets;

    public LogisticLossWithBias(
        Variable<Matrix> weights,
        Variable<Scalar> bias,
        Variable<Matrix> predictions,
        Variable<Matrix> features,
        Variable<Vector> targets
    ) {
        super(List.of(weights, bias, features, targets), scalar());

        validateVectorDimensions(weights.dimensions(), features.dimension(COLUMNS_INDEX));
        validateVectorDimensions(predictions.dimensions(), features.dimension(ROWS_INDEX));
        validateVectorDimensions(targets.dimensions(), features.dimension(ROWS_INDEX));

        this.weights = weights;
        this.bias = bias;
        this.predictions = predictions;
        this.features = features;
        this.targets = targets;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        ctx.forward(predictions);
        var predVector = ctx.data(predictions);
        var targetVector = ctx.data(targets);

        var numberOfExamples = targetVector.length();

        double result = 0;
        for(int idx = 0; idx < numberOfExamples; idx++) {
            var predicted = predVector.dataAt(idx);
            var target = targetVector.dataAt(idx);
            double v1 = target * log(predicted);
            double v2 = (1.0 - target) * log(1.0 - predicted);

            if (predicted == 0) {
                result += v2;
            }
            else if (predicted == 1.0) {
                result += v1;
            } else {
                result += v1 + v2;
            }
        }

        return new Scalar(-result / numberOfExamples);
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent == weights) {
            ctx.forward(predictions);
            var predVector = ctx.data(predictions);
            var targetVector = ctx.data(targets);
            var weightsVector = ctx.data(weights);
            var featuresTensor = ctx.data(features);
            var gradient = weightsVector.createWithSameDimensions();
            int featureCount = weightsVector.cols();
            int numberOfExamples = targetVector.length();

            for (int idx = 0; idx < numberOfExamples; idx++) {
                double errorPerNode = (predVector.dataAt(idx) - targetVector.dataAt(idx)) / numberOfExamples;
                for (int feature = 0; feature < featureCount; feature++) {
                    gradient.addDataAt(feature, errorPerNode * featuresTensor.dataAt(idx * featureCount + feature));
                }
            }
            return gradient;
        } else if (parent == bias) {
            ctx.forward(predictions);
            var predVector = ctx.data(predictions);
            var targetVector = ctx.data(targets);
            var gradient = new Scalar(0);
            int numberOfExamples = targetVector.length();

            for (int idx = 0; idx < numberOfExamples; idx++) {
                double errorPerNode = (predVector.dataAt(idx) - targetVector.dataAt(idx)) / numberOfExamples;
                gradient.addDataAt(0, errorPerNode);
            }
            return gradient;
        } else {
            // assume other variables do not require gradient
            return ctx.data(parent).createWithSameDimensions();
        }
    }

    private void validateVectorDimensions(int[] dimensions, int vectorLength) {
        if (!isVector(dimensions) || totalSize(dimensions) != vectorLength) {
            throw new IllegalStateException(formatWithLocale(
                "Expected a vector of size %d. Got %s",
                vectorLength,
                totalSize(dimensions)
            ));
        }
    }
}
