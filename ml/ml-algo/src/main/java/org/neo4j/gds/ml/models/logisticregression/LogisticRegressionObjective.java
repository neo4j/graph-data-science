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
package org.neo4j.gds.ml.models.logisticregression;

import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.ReducedCrossEntropyLoss;
import org.neo4j.gds.ml.core.functions.Softmax;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.gradientdescent.Objective;
import org.neo4j.gds.ml.models.Features;

import java.util.List;

import static java.lang.Math.max;

public class LogisticRegressionObjective implements Objective<LogisticRegressionData> {
    private final LogisticRegressionClassifier classifier;
    private final double penalty;
    private final Features features;
    private final HugeIntArray labels;

    @SuppressWarnings({"PointlessArithmeticExpression", "UnnecessaryLocalVariable"})
    public static long sizeOfBatchInBytes(boolean isReduced, int batchSize, int numberOfFeatures, int numberOfClasses) {
        // perThread
        int normalizedNumberOfClasses = isReduced ? (numberOfClasses - 1) : numberOfClasses;
        var batchLocalWeightGradient = Weights.sizeInBytes(normalizedNumberOfClasses, numberOfFeatures);
        var targets = Matrix.sizeInBytes(batchSize, 1);
        var weightedFeatures = MatrixMultiplyWithTransposedSecondOperand.sizeInBytes(
            batchSize,
            normalizedNumberOfClasses
        );
        var softMax = Softmax.sizeInBytes(batchSize, numberOfClasses);
        var unpenalizedLoss = ReducedCrossEntropyLoss.sizeInBytes();
        var l2norm = L2NormSquared.sizeInBytesOfApply();
        var constantScale = l2norm;
        var elementSum = constantScale;

        long sizeOfPredictionsVariableInBytes = LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(
            batchSize,
            numberOfFeatures,
            numberOfClasses,
            normalizedNumberOfClasses
        );

        long sizeOfComputationGraphForTrainEpoch =
            1 * targets +
            1 * weightedFeatures + // gradient
            1 * softMax +          // gradient
            2 * unpenalizedLoss +  // data and gradient
            2 * l2norm +           // data and gradient
            2 * constantScale +    // data and gradient
            2 * elementSum +       // data and gradient
            sizeOfPredictionsVariableInBytes +
            batchLocalWeightGradient;

        var sizeOfComputationGraphForEvaluateLoss =
            1 * targets +
            1 * weightedFeatures + // gradient
            1 * softMax +          // gradient
            1 * unpenalizedLoss +  // data
            1 * l2norm +           // data
            1 * constantScale +    // data
            1 * elementSum +
            sizeOfPredictionsVariableInBytes;

        return max(sizeOfComputationGraphForTrainEpoch, sizeOfComputationGraphForEvaluateLoss);
    }

    public LogisticRegressionObjective(
        LogisticRegressionClassifier classifier,
        double penalty,
        Features features,
        HugeIntArray labels
    ) {
        this.classifier = classifier;
        this.penalty = penalty;
        this.features = features;
        this.labels = labels;

        assert features.size() > 0;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(classifier.data().weights(), classifier.data().bias());
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        var unpenalizedLoss = crossEntropyLoss(batch);
        var penaltyVariable = penaltyForBatch(batch, trainSize);
        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    ConstantScale<Scalar> penaltyForBatch(Batch batch, long trainSize) {
        return new ConstantScale<>(new L2NormSquared<>(modelData().weights()), batch.size() * penalty / trainSize);
    }

    ReducedCrossEntropyLoss crossEntropyLoss(Batch batch) {
        var batchLabels = batchLabelVector(batch);
        var batchFeatures = Objective.batchFeatureMatrix(batch, features);
        var predictions = classifier.predictionsVariable(batchFeatures);
        return new ReducedCrossEntropyLoss(
            predictions,
            classifier.data().weights(),
            classifier.data().bias(),
            batchFeatures,
            batchLabels,
            0
        );
    }

    @Override
    public LogisticRegressionData modelData() {
        return classifier.data();
    }

    Constant<Vector> batchLabelVector(Batch batch) {
        var batchedTargets = new Vector(batch.size());
        int batchOffset = 0;

        var batchIterator = batch.elementIds();

        while (batchIterator.hasNext()) {
            long elementId = batchIterator.nextLong();
            batchedTargets.setDataAt(batchOffset++, labels.get(elementId));
        }

        return new Constant<>(batchedTargets);
    }
}
