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

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.gradientdescent.Training;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.models.Trainer;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.function.Supplier;

import static org.neo4j.gds.models.logisticregression.LogisticRegressionData.standard;
import static org.neo4j.gds.models.logisticregression.LogisticRegressionData.withReducedClassCount;

public final class LogisticRegressionTrainer implements Trainer {

    private final ReadOnlyHugeLongArray trainSet;
    private final LogisticRegressionTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final LocalIdMap classIdMap;
    private final boolean reduceClassCount;
    private final int concurrency;

    public static MemoryEstimation estimate(LogisticRegressionTrainConfig llrConfig, MemoryRange linkFeatureDimension) {
        return MemoryEstimations.builder("train model")
            .add("model data", LogisticRegressionData.memoryEstimationBinaryReduced(linkFeatureDimension))
            .add("update weights", Training.memoryEstimation(linkFeatureDimension, 1, 1))
            .perThread(
                "computation graph",
                linkFeatureDimension.apply(featureDim -> LogisticRegressionObjective.sizeOfBatchInBytes(
                    llrConfig.batchSize(),
                    Math.toIntExact(featureDim),
                    llrConfig.useBiasFeature()
                ))
            )
            .build();
    }

    public static MemoryEstimation memoryEstimation(
        int numberOfClasses,
        int numberOfFeatures,
        int batchSize
    ) {
        var CONSTANT_NUMBER_OF_WEIGHTS_IN_MODEL_DATA = 1;
        return MemoryEstimations.builder(LogisticRegressionTrainer.class)
            .add("model data", LogisticRegressionData.memoryEstimation(numberOfClasses, numberOfFeatures, true))
            .add("training", Training.memoryEstimation(numberOfFeatures, numberOfClasses, CONSTANT_NUMBER_OF_WEIGHTS_IN_MODEL_DATA))
            .perThread("computation graph", sizeInBytesOfComputationGraph(batchSize, numberOfFeatures, numberOfClasses))
            .build();
    }

    private static long sizeInBytesOfComputationGraph(int batchSize, int numberOfFeatures, int numberOfClasses) {
        return LogisticRegressionObjective.sizeOfBatchInBytes(batchSize, numberOfFeatures, numberOfClasses);
    }


    public LogisticRegressionTrainer(
        ReadOnlyHugeLongArray trainSet,
        int concurrency,
        LogisticRegressionTrainConfig trainConfig,
        LocalIdMap classIdMap,
        boolean reduceClassCount,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.trainSet = trainSet;
        this.concurrency = concurrency;
        this.trainConfig = trainConfig;
        this.classIdMap = classIdMap;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.reduceClassCount = reduceClassCount;
    }

    @Override
    public LogisticRegressionClassifier train(Features features, HugeLongArray labels) {
        var data = reduceClassCount
            ? withReducedClassCount(features.get(0).length, trainConfig.useBiasFeature(), classIdMap)
            : standard(features.get(0).length, trainConfig.useBiasFeature(), classIdMap);
        var classifier = new LogisticRegressionClassifier(data);
        var objective = new LogisticRegressionObjective(classifier, trainConfig.penalty(), features, labels);
        var training = new Training(trainConfig, progressTracker, trainSet.size(), terminationFlag);
        Supplier<BatchQueue> queueSupplier = () -> new HugeBatchQueue(trainSet, trainConfig.batchSize());

        training.train(objective, queueSupplier, concurrency);

        return classifier;
    }

}
