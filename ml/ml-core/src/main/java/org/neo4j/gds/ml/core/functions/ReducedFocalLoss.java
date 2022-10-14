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

import static org.neo4j.gds.ml.core.Dimensions.scalar;

/**
 * Computes focal loss given weights, bias, predictions, features and labels,
 * where it is assumed that predictions contain only values for all classes but the last one,
 * in practice, the output of ReducedSoftmax.
 */
public class ReducedFocalLoss extends AbstractVariable<Scalar> {

    private final Variable<Matrix> predictions;
    private final Variable<Matrix> weights;
    private final Weights<Vector> bias;
    private final Variable<Matrix> features;
    private final Variable<Vector> labels;
    private final double focusWeight;

    public ReducedFocalLoss(
        Variable<Matrix> predictions,
        Variable<Matrix> weights,
        Weights<Vector> bias,
        Variable<Matrix> features,
        Variable<Vector> labels,
        double focusWeight
    ) {
        super(
            List.of(weights, features, labels, bias),
            scalar()
        );

        this.weights = weights;
        this.predictions = predictions;
        this.features = features;
        this.labels = labels;
        this.bias = bias;
        this.focusWeight = focusWeight;
    }

    public static long sizeInBytes() {
        return Scalar.sizeInBytes();
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        // manually call forward as `predictions` is not registered as a parent
        var predictionsMatrix = ctx.forward(predictions);
        var labelsVector = ctx.data(labels);

        double result = 0;
        for (int row = 0; row < labelsVector.totalSize(); row++) {
            var trueClass = (int) labelsVector.dataAt(row);
            var predictedProbabilityForTrueClass = predictionsMatrix.dataAt(row, trueClass);
            if (predictedProbabilityForTrueClass > 0) {
                double focalFactor = Math.pow(1 - predictedProbabilityForTrueClass, focusWeight);
                result += focalFactor * Math.log(predictedProbabilityForTrueClass);
            }
        }
        return new Scalar(-result / predictionsMatrix.rows());
    }

    @Override
    public Tensor<?> gradient(Variable<?> parent, ComputationContext ctx) {
        // manually call forward as `predictions` is not registered as a parent
        var predMatrix = ctx.forward(predictions);
        var labelsVector = ctx.data(labels);
        int numberOfExamples = labelsVector.length();

        var selfGradient = ctx.gradient(this).value();

        if (parent == weights) {
            var weightsMatrix = ctx.data(weights);
            var featureMatrix = ctx.data(features);
            var gradient = weightsMatrix.createWithSameDimensions();
            int featureCount = weightsMatrix.cols();
            int reducedClassCount = weightsMatrix.rows();

            for (int row = 0; row < numberOfExamples; row++) {
                int trueClass = (int) labelsVector.dataAt(row);
                for (int classIdx = 0; classIdx < reducedClassCount; classIdx++) {
                    double predictedClassProbability = predMatrix.dataAt(row, classIdx);
                    var indicatorIsTrueClass = trueClass == classIdx ? 1.0 : 0.0;
                    var errorPerExample = (predictedClassProbability - indicatorIsTrueClass) / numberOfExamples;

                    var predictedProbabilityForTrueClass = 0.0;
                    if (trueClass == classIdx) {
                        predictedProbabilityForTrueClass = predictedClassProbability;
                    }  else {
                        //TODO modify for non-binary
                        predictedProbabilityForTrueClass = 1.0 - predictedClassProbability;
                    }

                    var focalLossPerExample = (focusWeight * Math.pow(1.0-predictedProbabilityForTrueClass, focusWeight-1.0) * Math.log(predictedProbabilityForTrueClass)
                                               - Math.pow(1.0-predictedProbabilityForTrueClass,focusWeight)/predictedProbabilityForTrueClass)
                                              * (predictedProbabilityForTrueClass * (indicatorIsTrueClass - predictedClassProbability)) / numberOfExamples;

                    for (int feature = 0; feature < featureCount; feature++) {
                        gradient.addDataAt(classIdx, feature, selfGradient * focalLossPerExample * featureMatrix.dataAt(row, feature));
                    }
                }
            }
            return gradient;
        } else if (parent == bias) {
            var biasVector = ctx.data(parent);
            var gradient = biasVector.createWithSameDimensions();
            int reducedClassCount = biasVector.totalSize();

            for (int row = 0; row < numberOfExamples; row++) {
                int trueClass = (int) labelsVector.dataAt(row);
                for (int classIdx = 0; classIdx < reducedClassCount; classIdx++) {
                    double predictedClassProbability = predMatrix.dataAt(row, classIdx);
                    var indicatorIsTrueClass = trueClass == classIdx ? 1.0 : 0.0;
                    var errorPerExample = (predictedClassProbability - indicatorIsTrueClass) / numberOfExamples;

                    var predictedProbabilityForTrueClass = 0.0;
                    if (trueClass == classIdx) {
                        predictedProbabilityForTrueClass = predictedClassProbability;
                    }  else {
                        //TODO modify for non-binary
                        predictedProbabilityForTrueClass = 1.0 - predictedClassProbability;
                    }

                    var focalLossPerExample = (focusWeight * Math.pow(1.0-predictedProbabilityForTrueClass, focusWeight-1.0) * Math.log(predictedProbabilityForTrueClass)
                                               - Math.pow(1.0-predictedProbabilityForTrueClass,focusWeight)/predictedProbabilityForTrueClass)
                                              * (predictedProbabilityForTrueClass * (indicatorIsTrueClass - predictedClassProbability)) / numberOfExamples;

                    gradient.addDataAt(classIdx, selfGradient * focalLossPerExample);
                }
            }
            return gradient;
        } else {
            throw new IllegalStateException(
                "The gradient should only be computed for the bias and the weights parents, but got " + parent.render());
        }
    }
}

