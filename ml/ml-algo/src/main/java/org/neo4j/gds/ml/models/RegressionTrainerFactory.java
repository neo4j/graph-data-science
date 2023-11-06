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

import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainer;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorTrainer;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorTrainerConfig;

import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class RegressionTrainerFactory {

    private RegressionTrainerFactory() {}

    public static RegressorTrainer create(
        TrainerConfig config,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        LogLevel messageLogLevel,
        int concurrency,
        Optional<Long> randomSeed
    ) {
        switch (config.method()) {
            case LinearRegression: {
                return new LinearRegressionTrainer(
                    concurrency,
                    (LinearRegressionTrainConfig) config,
                    terminationFlag,
                    progressTracker,
                    messageLogLevel
                );
            }
            case RandomForestRegression: {
                return new RandomForestRegressorTrainer(
                    concurrency,
                    (RandomForestRegressorTrainerConfig) config,
                    randomSeed,
                    terminationFlag,
                    progressTracker,
                    messageLogLevel
                );
            }
            default:
                throw new IllegalStateException(formatWithLocale("Method %s is not a regression method", config.method()));
        }
    }
}
