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

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.gradientdescent.Training;
import org.neo4j.gds.ml.models.Features;

import java.util.function.Supplier;

public final class LinearRegressionTrainer {

    private final int concurrency;
    private final LinearRegressionTrainConfig trainConfig;

    public LinearRegressionTrainer(int concurrency) {
        this.concurrency = concurrency;
        this.trainConfig = LinearRegressionTrainConfig.DEFAULT;
    }

    public LinearRegressor train(Features features, HugeDoubleArray targets, ReadOnlyHugeLongArray trainSet) {
        var objective = new LinearRegressionObjective(features, targets);
        Supplier<BatchQueue> queueSupplier = () -> new HugeBatchQueue(trainSet, trainConfig.batchSize());

        var training = new Training(trainConfig, ProgressTracker.NULL_TRACKER, trainSet.size(), TerminationFlag.RUNNING_TRUE);
        training.train(objective, queueSupplier, concurrency);

        return new LinearRegressor(objective.modelData());
    }

}
