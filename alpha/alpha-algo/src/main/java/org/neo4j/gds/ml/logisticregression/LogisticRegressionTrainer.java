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
package org.neo4j.gds.ml.logisticregression;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.Trainer;
import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;

import java.util.Optional;
import java.util.function.Supplier;

public final class LogisticRegressionTrainer implements Trainer {

    private final ReadOnlyHugeLongArray trainSet;
    private final LinkLogisticRegressionTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final LocalIdMap classIdMap;
    private final int concurrency;

    public static MemoryEstimation estimate(LinkLogisticRegressionTrainConfig llrConfig, MemoryRange linkFeatureDimension) {
        return MemoryEstimations.builder("train model")
            .add("model data", LogisticRegressionData.memoryEstimation(linkFeatureDimension))
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

    public LogisticRegressionTrainer(
        ReadOnlyHugeLongArray trainSet,
        int concurrency,
        LinkLogisticRegressionTrainConfig trainConfig,
        LocalIdMap classIdMap,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.trainSet = trainSet;
        this.concurrency = concurrency;
        this.trainConfig = trainConfig;
        this.classIdMap = classIdMap;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public LogisticRegressionClassifier train(Features features, HugeLongArray labels) {
        var data = LogisticRegressionData.of(features.get(0).length, trainConfig.useBiasFeature(), classIdMap);
        var classifier = new LogisticRegressionClassifier(data);
        var objective = new LogisticRegressionObjective(classifier, trainConfig.penalty(), features, labels);
        var training = new Training(trainConfig, progressTracker, features.size(), terminationFlag);
        Supplier<BatchQueue> queueSupplier = () -> new HugeBatchQueue(trainSet, trainConfig.batchSize());

        training.train(objective, queueSupplier, concurrency);

        return classifier;
    }

    @ValueClass
    public interface LogisticRegressionData extends ClassifierData {
        static MemoryEstimation memoryEstimation(MemoryRange linkFeatureDimension) {
            return MemoryEstimations.builder(LogisticRegressionData.class)
                .fixed("weights", linkFeatureDimension.apply(featureDim -> Weights.sizeInBytes(
                    1,
                    Math.toIntExact(featureDim)
                )))
                .fixed("bias", Weights.sizeInBytes(1, 1))
                .build();
        }

        Weights<Matrix> weights();
        Optional<Weights<Scalar>> bias();
        LocalIdMap classIdMap();

        static LogisticRegressionData of(int numberOfLinkFeatures, boolean useBias, LocalIdMap classIdMap) {
            // this is an optimization where "virtually" add a weight of 0.0 for the last class
            var weights = Weights.ofMatrix(classIdMap.size() - 1, numberOfLinkFeatures);

            var bias = useBias
                ? Optional.of(Weights.ofScalar(0))
                : Optional.<Weights<Scalar>>empty();

            return ImmutableLogisticRegressionData.builder().weights(weights).classIdMap(classIdMap).bias(bias).build();
        }
    }
}
