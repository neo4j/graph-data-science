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

import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.mlp.MLPClassifier;
import org.neo4j.gds.ml.models.mlp.MLPClassifierData;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifier;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierData;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainerConfig;

import java.util.function.LongUnaryOperator;

public final class ClassifierFactory {

    private ClassifierFactory() {}

    public static Classifier create(
        Classifier.ClassifierData classifierData
    ) {
        return switch (classifierData.trainerMethod()) {
            case LogisticRegression -> LogisticRegressionClassifier.from((LogisticRegressionData) classifierData);
            case RandomForestClassification -> new RandomForestClassifier((RandomForestClassifierData) classifierData);
            case MLPClassification -> new MLPClassifier((MLPClassifierData) classifierData);
            case LinearRegression, RandomForestRegression -> throw new IllegalStateException("No such classifier.");
        };
    }

    public static MemoryRange runtimeOverheadMemoryEstimation(
        TrainingMethod method,
        int batchSize,
        int numberOfClasses,
        int featureDimension,
        boolean isReduced
    ) {
        return switch (method) {
            case LogisticRegression -> LogisticRegressionClassifier.runtimeOverheadMemoryEstimation(
                batchSize,
                featureDimension,
                numberOfClasses,
                isReduced
            );
            case RandomForestClassification -> RandomForestClassifier.runtimeOverheadMemoryEstimation(numberOfClasses);
            case MLPClassification -> MemoryRange.empty();
            case RandomForestRegression, LinearRegression -> throw new IllegalStateException("No such classifier.");
        };
    }

    public static MemoryEstimation dataMemoryEstimation(
        TrainerConfig trainerConfig,
        LongUnaryOperator numberOfTrainingSamples,
        int numberOfClasses,
        int featureDimension,
        boolean isReduced
    ) {
        return switch (trainerConfig.method()) {
            case LogisticRegression -> LogisticRegressionData.memoryEstimation(
                isReduced,
                numberOfClasses,
                MemoryRange.of(featureDimension)
            );
            case RandomForestClassification -> RandomForestClassifierData.memoryEstimation(
                numberOfTrainingSamples,
                (RandomForestTrainerConfig) trainerConfig
            );
            case MLPClassification -> MemoryEstimations.empty();
            case LinearRegression, RandomForestRegression -> throw new IllegalStateException("No such classifier.");
        };
    }
}
