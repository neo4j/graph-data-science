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
package org.neo4j.gds.ml.models;

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.ClassificationRandomForestPredictor;
import org.neo4j.gds.ml.models.randomforest.RandomForestData;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfig;

public final class ClassifierFactory {

    private ClassifierFactory() {}

    public static Classifier create(
        Classifier.ClassifierData classifierData
    ) {
        switch (classifierData.trainerMethod()) {
            case LogisticRegression:
                return LogisticRegressionClassifier.from((LogisticRegressionData) classifierData);
            case RandomForest:
                return new ClassificationRandomForestPredictor((RandomForestData) classifierData);
            default:
                throw new IllegalStateException("No such classifier.");
        }
    }

    public static MemoryRange memoryEstimation(
        TrainerConfig trainerConfig,
        long numberOfTrainingSamples,
        int numberOfClasses,
        int featureDimension,
        boolean isReduced
    ) {
        switch (TrainingMethod.valueOf(trainerConfig.methodName())) {
            case LogisticRegression:
                int normalizedNumberOfClasses = isReduced ? (numberOfClasses - 1) : numberOfClasses;
                return MemoryRange.of(LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(
                    ((LogisticRegressionTrainConfig) trainerConfig).batchSize(),
                    featureDimension,
                    numberOfClasses,
                    normalizedNumberOfClasses
                ));
            case RandomForest:
                return ClassificationRandomForestPredictor.memoryEstimation(
                    numberOfTrainingSamples,
                    numberOfClasses,
                    (RandomForestTrainConfig) trainerConfig
                );
            default:
                throw new IllegalStateException("No such classifier.");
        }
    }
}
