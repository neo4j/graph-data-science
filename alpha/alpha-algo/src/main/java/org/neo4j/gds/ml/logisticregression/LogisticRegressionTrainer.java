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
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;

import java.util.Optional;
import java.util.function.Supplier;

public final class LogisticRegressionTrainer implements Trainer {

    private final ReadOnlyHugeLongArray trainSet;
    private final LinkLogisticRegressionTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;
    private final int concurrency;

    public static MemoryEstimation estimate(LinkLogisticRegressionTrainConfig llrConfig, MemoryRange linkFeatureDimension) {
        return MemoryEstimations.builder("train model")
            .add("model data", LinkLogisticRegressionData.memoryEstimation(linkFeatureDimension))
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
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.trainSet = trainSet;
        this.trainConfig = trainConfig;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
    }

    @Override
    public LogisticRegressionClassifier train(Features features, HugeLongArray labels) {
        var data = LogisticRegressionData.of(features.get(0).length, labels, trainConfig.useBiasFeature());
        var classifier = new LogisticRegressionClassifier(data);
        var objective = new LogisticRegressionObjective(classifier, trainConfig.penalty(), features, labels);
        var training = new Training(trainConfig, progressTracker, features.size(), terminationFlag);
        Supplier<BatchQueue> queueSupplier = () -> new HugeBatchQueue(trainSet, trainConfig.batchSize());

        training.train(objective, queueSupplier, concurrency);

        return classifier;
    }

    @ValueClass
    public interface LogisticRegressionData extends ClassifierData {
        Weights<Matrix> weights();
        Optional<Weights<Scalar>> bias();
        LocalIdMap classIdMap();

        static LogisticRegressionData of(int numberOfLinkFeatures, HugeLongArray labels, boolean useBias) {
            var classIdMap = new LocalIdMap();
            for (long i = 0; i < labels.size(); i++) {
                classIdMap.toMapped(labels.get(i));
            }

            var weights = Weights.ofMatrix(classIdMap.size(), numberOfLinkFeatures);

            var bias = useBias
                ? Optional.of(Weights.ofScalar(0))
                : Optional.<Weights<Scalar>>empty();

            return ImmutableLogisticRegressionData.builder().weights(weights).classIdMap(classIdMap).bias(bias).build();
        }
    }
}
