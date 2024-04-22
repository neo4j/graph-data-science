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

import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.MatrixVectorSum;
import org.neo4j.gds.ml.core.functions.ReducedSoftmax;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.functions.Softmax;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.gradientdescent.Objective;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;

import static org.neo4j.gds.ml.core.Dimensions.matrix;

public final class LogisticRegressionClassifier implements Classifier {

    private final LogisticRegressionData data;
    private final LogisticRegressionPredictionStrategy predictionStrategy;

    private LogisticRegressionClassifier(
        LogisticRegressionData data,
        LogisticRegressionPredictionStrategy predictionStrategy
    ) {
        this.data = data;
        this.predictionStrategy = predictionStrategy;
    }

    interface LogisticRegressionPredictionStrategy {
        double[] predictProbabilities(double[] features, LogisticRegressionClassifier classifier);

        static LogisticRegressionPredictionStrategy binary() {
            return (features, classifier) -> {
                var affinity = 0D;
                var weights = classifier.data().weights().data();
                for (int i = 0; i < features.length; i++) {
                    affinity += weights.dataAt(i) * features[i];
                }
                var sigmoid = Sigmoid.sigmoid(affinity + classifier.data().bias().data().dataAt(0));

                return new double[]{sigmoid, 1 - sigmoid};
            };
        }

        static LogisticRegressionPredictionStrategy multiClass() {
            return (features, classifier) -> {
                ComputationContext ctx = new ComputationContext();
                Constant<Matrix> featuresVariable = Constant.matrix(features, 1, features.length);
                Variable<Matrix> predictionsVariable = classifier.predictionsVariable(featuresVariable);
                return ctx.forward(predictionsVariable).data();
            };
        }
    }

    public static LogisticRegressionClassifier from(LogisticRegressionData data) {
        LogisticRegressionPredictionStrategy predictionStrategy;
        if (data.numberOfClasses() == 2 && data.weights().data().rows() == 1) {
            // this case corresponds to 2 classes and 1 weight
            // that 'happens to be' the LinkPrediction case
            // it could also be used for NodeClassification if we use reduced weights matrix for it
            predictionStrategy = LogisticRegressionPredictionStrategy.binary();
        } else {
            predictionStrategy = LogisticRegressionPredictionStrategy.multiClass();
        }
        return new LogisticRegressionClassifier(data, predictionStrategy);
    }

    public static long sizeOfPredictionsVariableInBytes(
        int batchSize,
        int numberOfFeatures,
        int numberOfClasses,
        int normalizedNumberOfClasses
    ) {
        var dimensionsOfFirstMatrix = matrix(batchSize, numberOfFeatures);
        var softmaxSize = numberOfClasses == normalizedNumberOfClasses
            ? Softmax.sizeInBytes(batchSize, numberOfClasses)
            : ReducedSoftmax.sizeInBytes(batchSize, numberOfClasses);
        return
            sizeOfFeatureExtractorsInBytes(numberOfFeatures) +
            Constant.sizeInBytes(dimensionsOfFirstMatrix) +
            MatrixMultiplyWithTransposedSecondOperand.sizeInBytes(
                batchSize,
                normalizedNumberOfClasses
            ) +
            softmaxSize;
    }

    public static MemoryRange runtimeOverheadMemoryEstimation(
        int batchSize,
        int featureDimension,
        int numberOfClasses,
        boolean isReduced
    ) {
        int normalizedNumberOfClasses = isReduced ? (numberOfClasses - 1) : numberOfClasses;

        return MemoryRange.of(
            LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(
                batchSize,
                featureDimension,
                numberOfClasses,
                normalizedNumberOfClasses
            )
        );
    }

    private static long sizeOfFeatureExtractorsInBytes(int numberOfFeatures) {
        return FeatureExtraction.memoryUsageInBytes(numberOfFeatures);
    }

    @Override
    public double[] predictProbabilities(double[] features) {
        return predictionStrategy.predictProbabilities(features, this);
    }

    @Override
    public Matrix predictProbabilities(Batch batch, Features features) {
        ComputationContext ctx = new ComputationContext();
        return ctx.forward(predictionsVariable(Objective.batchFeatureMatrix(batch, features)));
    }

    Variable<Matrix> predictionsVariable(Constant<Matrix> batchFeatures) {
        var weights = data.weights();
        var weightedFeatures = MatrixMultiplyWithTransposedSecondOperand.of(
            batchFeatures,
            weights
        );
        var softmaxInput = new MatrixVectorSum(weightedFeatures, data.bias());
        return weights.data().rows() == numberOfClasses()
            ? new Softmax(softmaxInput)
            : new ReducedSoftmax(softmaxInput);
    }

    @Override
    public LogisticRegressionData data() {
        return data;
    }
}
