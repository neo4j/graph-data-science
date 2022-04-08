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

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainer;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainer;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainerConfig;

import java.util.Optional;
import java.util.function.LongUnaryOperator;

public class TrainerFactory {

    private TrainerFactory() {}

    public static ClassifierTrainer create(
        TrainerConfig config,
        LocalIdMap classIdMap,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        int concurrency,
        Optional<Long> randomSeed,
        boolean reduceClassCount
    ) {
        switch (TrainingMethod.valueOf(config.methodName())) {
            case LogisticRegression: {
                return new LogisticRegressionTrainer(
                    concurrency,
                    (LogisticRegressionTrainConfig) config,
                    classIdMap,
                    reduceClassCount,
                    terminationFlag,
                    progressTracker
                );
            }
            case RandomForest: {
                return new RandomForestClassifierTrainer(
                    concurrency,
                    classIdMap,
                    (RandomForestTrainerConfig) config,
                    false,
                    randomSeed,
                    progressTracker
                );
            }
            default:
                throw new IllegalStateException("No such training method.");
        }
    }

    public static MemoryEstimation memoryEstimation(
        TrainerConfig config,
        LongUnaryOperator numberOfTrainingExamples,
        int numberOfClasses,
        MemoryRange featureDimension,
        boolean isReduced
    ) {
        switch (TrainingMethod.valueOf(config.methodName())) {
            case LogisticRegression:
                return LogisticRegressionTrainer.memoryEstimation(
                    isReduced,
                    numberOfClasses,
                    featureDimension,
                    ((LogisticRegressionTrainConfig) config).batchSize()
                );
            case RandomForest: {
                return RandomForestClassifierTrainer.memoryEstimation(
                    numberOfTrainingExamples,
                    numberOfClasses,
                    featureDimension,
                   (RandomForestTrainerConfig) config
                );
            }
            default:
                throw new IllegalStateException("No such training method.");
        }
    }
}
