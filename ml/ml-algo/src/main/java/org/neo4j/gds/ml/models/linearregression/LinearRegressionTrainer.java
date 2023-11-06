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
package org.neo4j.gds.ml.models.linearregression;

import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.gradientdescent.Training;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.RegressorTrainer;

import java.util.function.Supplier;

public final class LinearRegressionTrainer implements RegressorTrainer {

    private final int concurrency;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private final LogLevel messageLogLevel;
    private final LinearRegressionTrainConfig trainConfig;

    public LinearRegressionTrainer(
        int concurrency,
        LinearRegressionTrainConfig config,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        LogLevel messageLogLevel
    ) {
        this.concurrency = concurrency;
        this.trainConfig = config;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.messageLogLevel = messageLogLevel;
    }

    @Override
    public LinearRegressor train(Features features, HugeDoubleArray targets, ReadOnlyHugeLongArray trainSet) {
        var objective = new LinearRegressionObjective(features, targets, trainConfig.penalty());
        Supplier<BatchQueue> queueSupplier = () -> BatchQueue.fromArray(trainSet, trainConfig.batchSize());

        var training = new Training(trainConfig, progressTracker, messageLogLevel, trainSet.size(), terminationFlag);
        training.train(objective, queueSupplier, concurrency);

        return new LinearRegressor(objective.modelData());
    }

}
