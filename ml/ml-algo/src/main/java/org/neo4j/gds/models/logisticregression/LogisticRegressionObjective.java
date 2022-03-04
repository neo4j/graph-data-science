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
package org.neo4j.gds.models.logisticregression;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.gradientdescent.Objective;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.CrossEntropyLoss;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.L2NormSquared;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.ReducedCrossEntropyLoss;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;
import java.util.Optional;

import static java.lang.Math.max;

public class LogisticRegressionObjective implements Objective<LogisticRegressionData> {
    private final LogisticRegressionClassifier classifier;
    private final double penalty;
    private final Features features;
    private final HugeLongArray labels;

    //TODO: add support for number of classes and Fudge only in NC
    public static long sizeOfBatchInBytes(int batchSize, int featureDim, boolean useBias) {
        // we consider each variable in the computation graph
        var batchedTargets = Matrix.sizeInBytes(batchSize, 1);
        var features = Matrix.sizeInBytes(batchSize, featureDim);

        var weightedFeatures = MatrixMultiplyWithTransposedSecondOperand.sizeInBytes(
            Dimensions.matrix(batchSize, featureDim),
            Dimensions.matrix(1, featureDim)
        );

        var sigmoid = weightedFeatures;
        var unpenalizedLoss = Scalar.sizeInBytes();
        var l2norm = Scalar.sizeInBytes();
        var constantScale = Scalar.sizeInBytes();
        var elementSum = Scalar.sizeInBytes();

        var maybeBias = useBias ? weightedFeatures : 0;
        var penalty = l2norm + constantScale;

        // 2 * x == computing data and gradient for this computation variable
        return MemoryUsage.sizeOfInstance(LogisticRegressionClassifier.class) +
               Weights.sizeInBytes(1, featureDim) + // only gradient as data is the model data
               batchedTargets +
               features +
               2 * weightedFeatures +
               2 * maybeBias +
               2 * sigmoid +
               2 * unpenalizedLoss +
               2 * penalty +
               2 * elementSum;
    }

    //TODO: fix me and merge with above method
    @SuppressWarnings({"PointlessArithmeticExpression", "UnnecessaryLocalVariable"})
    public static long sizeOfBatchInBytes(int batchSize, int numberOfFeatures, int numberOfClasses) {
        // perThread
        var batchLocalWeightGradient = Weights.sizeInBytes(numberOfClasses, numberOfFeatures);
        var targets = Matrix.sizeInBytes(batchSize, 1);
        var weightedFeatures = MatrixMultiplyWithTransposedSecondOperand.sizeInBytes(
            Dimensions.matrix(batchSize, numberOfFeatures),
            Dimensions.matrix(numberOfClasses, numberOfFeatures)
        );
        var softMax = weightedFeatures;
        var unpenalizedLoss = CrossEntropyLoss.sizeInBytes();
        var l2norm = L2NormSquared.sizeInBytesOfApply();
        var constantScale = L2NormSquared.sizeInBytesOfApply(); //
        var elementSum = constantScale;

        long sizeOfPredictionsVariableInBytes = LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(
            batchSize,
            numberOfFeatures,
            numberOfClasses
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
        HugeLongArray labels
    ) {
        this.classifier = classifier;
        this.penalty = penalty;
        this.features = features;
        this.labels = labels;

        assert features.size() > 0;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        Optional<Weights<Vector>> bias = classifier.data().bias();
        if (bias.isPresent()) {
            return List.of(classifier.data().weights(), bias.get());
        } else {
            return List.of(classifier.data().weights());
        }
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        var unpenalizedLoss = crossEntropyLoss(batch);
        var penaltyVariable = penaltyForBatch(batch, trainSize);
        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    public ConstantScale<Scalar> penaltyForBatch(Batch batch, long trainSize) {
        return new ConstantScale<>(new L2NormSquared(modelData().weights()), batch.size() * penalty / trainSize);
    }

    public ReducedCrossEntropyLoss crossEntropyLoss(Batch batch) {
        var batchLabels = batchLabelVector(batch, classifier.classIdMap());
        var batchFeatures = LogisticRegressionClassifier.batchFeatureMatrix(batch, features);
        var predictions = classifier.predictionsVariable(batchFeatures);
        return new ReducedCrossEntropyLoss(
            predictions,
            classifier.data().weights(),
            classifier.data().bias(),
            batchFeatures,
            batchLabels
        );
    }

    @Override
    public LogisticRegressionData modelData() {
        return classifier.data();
    }

    Constant<Vector> batchLabelVector(Batch batch, LocalIdMap localIdMap) {
        var batchedTargets = new Vector(batch.size());
        var batchOffset = new MutableInt();

        batch.nodeIds().forEach(elementId ->
            batchedTargets.setDataAt(
                batchOffset.getAndIncrement(),
                localIdMap.toMapped(labels.get(elementId))
            )
        );

        return new Constant<>(batchedTargets);
    }
}
