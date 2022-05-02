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

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.gradientdescent.Training;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.Features;

import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

import static org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData.standard;
import static org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData.withReducedClassCount;

public final class LogisticRegressionTrainer implements ClassifierTrainer {

    private final LogisticRegressionTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final LocalIdMap classIdMap;
    private final boolean reduceClassCount;
    private final int concurrency;

    public static MemoryEstimation memoryEstimation(
        boolean isReduced,
        int numberOfClasses,
        MemoryRange featureDimension,
        int batchSize,
        LongUnaryOperator numberOfTrainingExamples
    ) {
        return MemoryEstimations.builder("train logistic regression")
            .add("model data", LogisticRegressionData.memoryEstimation(isReduced, numberOfClasses, featureDimension))
            .add("update weights", Training.memoryEstimation(featureDimension, numberOfClasses))
            .perGraphDimension(
                "computation graph",
                (graphDimensions, concurrency) -> {
                    long actualTrainSetSize = numberOfTrainingExamples.applyAsLong(graphDimensions.nodeCount());
                    int numberOfConcurrentComputationGraphs = (int) Math.min(concurrency, Math.ceil((double) actualTrainSetSize / batchSize));
                    return featureDimension.apply(dim ->
                        sizeInBytesOfComputationGraph(
                            isReduced,
                            batchSize,
                            (int) dim,
                            numberOfClasses
                        )
                    ).times(numberOfConcurrentComputationGraphs);
                }
            )
            .build();
    }

    private static long sizeInBytesOfComputationGraph(boolean isReduced, int batchSize, int numberOfFeatures, int numberOfClasses) {
        return LogisticRegressionObjective.sizeOfBatchInBytes(isReduced, batchSize, numberOfFeatures, numberOfClasses);
    }


    public LogisticRegressionTrainer(
        int concurrency,
        LogisticRegressionTrainConfig trainConfig,
        LocalIdMap classIdMap,
        boolean reduceClassCount,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.concurrency = concurrency;
        this.trainConfig = trainConfig;
        this.classIdMap = classIdMap;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.reduceClassCount = reduceClassCount;
    }

    @Override
    public LogisticRegressionClassifier train(Features features, HugeLongArray labels, ReadOnlyHugeLongArray trainSet) {
        var data = reduceClassCount
            ? withReducedClassCount(features.featureDimension(), classIdMap)
            : standard(features.featureDimension(), classIdMap);
        var classifier = LogisticRegressionClassifier.from(data);
        var objective = new LogisticRegressionObjective(classifier, trainConfig.penalty(), features, labels);
        var training = new Training(trainConfig, progressTracker, trainSet.size(), terminationFlag);
        Supplier<BatchQueue> queueSupplier = () -> BatchQueue.fromArray(trainSet, trainConfig.batchSize());

        training.train(objective, queueSupplier, concurrency);

        return classifier;
    }

}
