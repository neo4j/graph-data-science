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

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainer;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfig;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainer;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainer;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.function.LongUnaryOperator;

public final class ClassifierTrainerFactory {

    private ClassifierTrainerFactory() {}

    public static ClassifierTrainer create(
        Log log,
        TrainerConfig config,
        int numberOfClasses,
        TerminationFlag terminationFlag,
        Concurrency concurrency,
        Optional<Long> randomSeed,
        boolean reduceClassCount,
        ModelSpecificMetricsHandler metricsHandler
    ) {
        return switch (config.method()) {
            case LogisticRegression -> new LogisticRegressionTrainer(
                log,
                concurrency,
                (LogisticRegressionTrainConfig) config,
                numberOfClasses,
                reduceClassCount,
                terminationFlag
            );
            case RandomForestClassification -> new RandomForestClassifierTrainer(
                log,
                concurrency,
                numberOfClasses,
                (RandomForestClassifierTrainerConfig) config,
                randomSeed,
                terminationFlag,
                metricsHandler
            );
            case MLPClassification -> new MLPClassifierTrainer(
                log,
                numberOfClasses,
                (MLPClassifierTrainConfig) config,
                randomSeed,
                terminationFlag,
                concurrency
            );
            default -> throw new IllegalStateException("No such training method.");
        };
    }

    public static MemoryEstimation memoryEstimation(
        TrainerConfig config,
        LongUnaryOperator numberOfTrainingExamples,
        int numberOfClasses,
        MemoryRange featureDimension,
        boolean isReduced
    ) {
        return switch (config.method()) {
            case LogisticRegression -> LogisticRegressionTrainer.memoryEstimation(
                isReduced,
                numberOfClasses,
                featureDimension,
                ((LogisticRegressionTrainConfig) config).batchSize(),
                numberOfTrainingExamples
            );
            case RandomForestClassification -> RandomForestClassifierTrainer.memoryEstimation(
                numberOfTrainingExamples,
                numberOfClasses,
                featureDimension,
                (RandomForestClassifierTrainerConfig) config
            );
            case MLPClassification -> MemoryEstimations.empty();
            default -> throw new IllegalStateException("No such training method.");
        };
    }
}
