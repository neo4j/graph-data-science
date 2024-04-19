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
package org.neo4j.gds.ml.models.randomforest;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.decisiontree.DecisionTreeRegressorTrainer;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainerConfig;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainerConfigImpl;
import org.neo4j.gds.ml.decisiontree.FeatureBagger;
import org.neo4j.gds.ml.decisiontree.ImpurityCriterion;
import org.neo4j.gds.ml.decisiontree.SplitMeanSquaredError;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.RegressorTrainer;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RandomForestRegressorTrainer implements RegressorTrainer {

    private final RandomForestRegressorTrainerConfig config;
    private final Concurrency concurrency;
    private final SplittableRandom random;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private final LogLevel messageLogLevel;

    public RandomForestRegressorTrainer(
        Concurrency concurrency,
        RandomForestRegressorTrainerConfig config,
        Optional<Long> randomSeed,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker,
        LogLevel messageLogLevel
    ) {
        this.config = config;
        this.concurrency = concurrency;
        this.random = new SplittableRandom(randomSeed.orElseGet(() -> new SplittableRandom().nextLong()));
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.messageLogLevel = messageLogLevel;
    }

    public static MemoryEstimation memoryEstimation(
        LongUnaryOperator numberOfTrainingSamples,
        MemoryRange featureDimension,
        RandomForestRegressorTrainerConfig config
    ) {
        int minNumberOfBaggedFeatures = (int) Math.ceil(config.maxFeaturesRatio((int) featureDimension.min) * featureDimension.min);
        int maxNumberOfBaggedFeatures = (int) Math.ceil(config.maxFeaturesRatio((int) featureDimension.max) * featureDimension.max);

        return MemoryEstimations.builder("Training")
            // estimating the final forest produced
            .add(RandomForestRegressorData.memoryEstimation(numberOfTrainingSamples, config))
            .rangePerNode(
                "Mean Squared Error Loss",
                nodeCount -> SplitMeanSquaredError.memoryEstimation()
            ).perGraphDimension(
                "Decision tree training",
                (dim, concurrency) ->
                    TrainDecisionTreeTask.memoryEstimation(
                        config,
                        numberOfTrainingSamples.applyAsLong(dim.nodeCount()),
                        minNumberOfBaggedFeatures,
                        config.numberOfSamplesRatio()
                    ).union(
                        TrainDecisionTreeTask.memoryEstimation(
                            config,
                            numberOfTrainingSamples.applyAsLong(dim.nodeCount()),
                            maxNumberOfBaggedFeatures,
                            config.numberOfSamplesRatio()
                        )
                    ).times(concurrency)
            )
            .build();
    }

    public RandomForestRegressor train(
        Features allFeatureVectors,
        HugeDoubleArray targets,
        ReadOnlyHugeLongArray trainSet
    ) {
        var decisionTreeTrainConfig = DecisionTreeTrainerConfigImpl.builder()
            .maxDepth(config.maxDepth())
            .minSplitSize(config.minSplitSize())
            .build();

        int numberOfDecisionTrees = config.numberOfDecisionTrees();
        var impurityCriterion = new SplitMeanSquaredError(targets);

        var numberOfTreesTrained = new AtomicInteger(0);

        var tasks = IntStream.range(0, numberOfDecisionTrees).mapToObj(unused ->
            new TrainDecisionTreeTask(
                decisionTreeTrainConfig,
                config,
                random.split(),
                allFeatureVectors,
                targets,
                impurityCriterion,
                trainSet,
                progressTracker,
                messageLogLevel,
                numberOfTreesTrained
            )
        ).collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        var decisionTrees = tasks.stream().map(TrainDecisionTreeTask::trainedTree).collect(Collectors.toList());

        return new RandomForestRegressor(decisionTrees, allFeatureVectors.featureDimension());
    }

    static class TrainDecisionTreeTask implements Runnable {

        private DecisionTreePredictor<Double> trainedTree;
        private final DecisionTreeTrainerConfig decisionTreeTrainConfig;
        private final RandomForestTrainerConfig randomForestTrainConfig;
        private final SplittableRandom random;
        private final Features allFeatureVectors;
        private final HugeDoubleArray targets;
        private final ImpurityCriterion impurityCriterion;
        private final ReadOnlyHugeLongArray trainSet;
        private final ProgressTracker progressTracker;
        private final LogLevel messageLogLevel;
        private final AtomicInteger numberOfTreesTrained;

        TrainDecisionTreeTask(
            DecisionTreeTrainerConfig decisionTreeTrainConfig,
            RandomForestTrainerConfig randomForestTrainConfig,
            SplittableRandom random,
            Features allFeatureVectors,
            HugeDoubleArray targets,
            ImpurityCriterion impurityCriterion,
            ReadOnlyHugeLongArray trainSet,
            ProgressTracker progressTracker,
            LogLevel messageLogLevel,
            AtomicInteger numberOfTreesTrained
        ) {
            this.decisionTreeTrainConfig = decisionTreeTrainConfig;
            this.randomForestTrainConfig = randomForestTrainConfig;
            this.random = random;
            this.allFeatureVectors = allFeatureVectors;
            this.targets = targets;
            this.impurityCriterion = impurityCriterion;
            this.trainSet = trainSet;
            this.progressTracker = progressTracker;
            this.messageLogLevel = messageLogLevel;
            this.numberOfTreesTrained = numberOfTreesTrained;
        }

        public static MemoryRange memoryEstimation(
            DecisionTreeTrainerConfig config,
            long numberOfTrainingSamples,
            int numberOfBaggedFeatures,
            double numberOfSamplesRatio
        ) {
            long usedNumberOfTrainingSamples = (long) Math.ceil(numberOfSamplesRatio * numberOfTrainingSamples);

            var bootstrappedDatasetEstimation = MemoryRange
                .of(HugeLongArray.memoryEstimation(usedNumberOfTrainingSamples))
                .add(MemoryUsage.sizeOfBitset(usedNumberOfTrainingSamples));

            return MemoryRange.of(sizeOfInstance(TrainDecisionTreeTask.class))
                .add(FeatureBagger.memoryEstimation(numberOfBaggedFeatures))
                .add(DecisionTreeRegressorTrainer.memoryEstimation(
                    config,
                    usedNumberOfTrainingSamples
                ))
                .add(bootstrappedDatasetEstimation);
        }

        public DecisionTreePredictor<Double> trainedTree() {
            return trainedTree;
        }

        @Override
        public void run() {
            var featureBagger = new FeatureBagger(
                random,
                allFeatureVectors.featureDimension(),
                randomForestTrainConfig.maxFeaturesRatio(allFeatureVectors.featureDimension())
            );

            var decisionTree = new DecisionTreeRegressorTrainer(
                impurityCriterion,
                allFeatureVectors,
                targets,
                decisionTreeTrainConfig,
                featureBagger
            );

            trainedTree = decisionTree.train(bootstrappedDataset());

            progressTracker.logMessage(
                messageLogLevel,
                formatWithLocale(
                    "trained decision tree %d out of %d",
                    numberOfTreesTrained.incrementAndGet(),
                    randomForestTrainConfig.numberOfDecisionTrees()
                )
            );
        }

        private ReadOnlyHugeLongArray bootstrappedDataset() {
            BitSet trainSetIndices = new BitSet(trainSet.size());
            ReadOnlyHugeLongArray allVectorsIndices;

            if (Double.compare(randomForestTrainConfig.numberOfSamplesRatio(), 0.0d) == 0) {
                // 0 => no sampling but take every vector
                allVectorsIndices = trainSet;
                trainSetIndices.set(1, trainSet.size());
            } else {
                allVectorsIndices = DatasetBootstrapper.bootstrap(
                    random,
                    randomForestTrainConfig.numberOfSamplesRatio(),
                    trainSet,
                    trainSetIndices
                );
            }

            // trainSetIndices unused until we add out-out-bag-error
            return allVectorsIndices;
        }
    }
}
