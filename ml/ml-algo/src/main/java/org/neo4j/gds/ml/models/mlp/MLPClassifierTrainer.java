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
package org.neo4j.gds.ml.models.mlp;

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.gradientdescent.Training;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.Features;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.function.Supplier;

public class MLPClassifierTrainer implements ClassifierTrainer {

    private final int numberOfClasses;

    private final MLPClassifierTrainConfig trainConfig;

    private final SplittableRandom random;

    private final ProgressTracker progressTracker;

    private final LogLevel messageLogLevel;

    private final TerminationFlag terminationFlag;

    private final int concurrency;

    public MLPClassifierTrainer(int numberOfClasses,
                                MLPClassifierTrainConfig trainConfig,
                                Optional<Long> randomSeed,
                                ProgressTracker progressTracker,
                                LogLevel messageLogLevel,
                                TerminationFlag terminationFlag,
                                int concurrency
    ) {
        this.numberOfClasses = numberOfClasses;
        this.trainConfig = trainConfig;
        this.random = new SplittableRandom(randomSeed.orElseGet(() -> new SplittableRandom().nextLong()));
        this.progressTracker = progressTracker;
        this.messageLogLevel = messageLogLevel;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
    }
    @Override
    public MLPClassifier train(Features features, HugeIntArray labels, ReadOnlyHugeLongArray trainSet) {
        var data = MLPClassifierData.create(numberOfClasses, features.featureDimension(), trainConfig.hiddenLayerSizes(), random);
        var classifier = new MLPClassifier(data);
        var objective = new MLPClassifierObjective(classifier, features, labels, trainConfig.penalty(), trainConfig.focusWeight());
        var training = new Training(trainConfig, progressTracker, messageLogLevel, trainSet.size(), terminationFlag);

        Supplier<BatchQueue> queueSupplier = () -> BatchQueue.fromArray(trainSet, trainConfig.batchSize());

        training.train(objective, queueSupplier, concurrency);

        return classifier;
    }
}
