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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.decisiontree.ClassifierImpurityCriterionType;
import org.neo4j.gds.ml.decisiontree.DecisionTreeClassifierTrainer;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainerConfig;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainerConfigImpl;
import org.neo4j.gds.ml.decisiontree.Entropy;
import org.neo4j.gds.ml.decisiontree.FeatureBagger;
import org.neo4j.gds.ml.decisiontree.GiniIndex;
import org.neo4j.gds.ml.decisiontree.ImpurityCriterion;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.metrics.classification.OutOfBagError;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.Features;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class RandomForestClassifierTrainer implements ClassifierTrainer {

    private final int numberOfClasses;
    private final RandomForestClassifierTrainerConfig config;
    private final int concurrency;
    private final SplittableRandom random;
    private final ProgressTracker progressTracker;
    private final LogLevel messageLogLevel;
    private final TerminationFlag terminationFlag;
    private Optional<Double> outOfBagError = Optional.empty();
    private final ModelSpecificMetricsHandler metricsHandler;

    public RandomForestClassifierTrainer(
        int concurrency,
        int numberOfClasses,
        RandomForestClassifierTrainerConfig config,
        Optional<Long> randomSeed,
        ProgressTracker progressTracker,
        LogLevel messageLogLevel,
        TerminationFlag terminationFlag,
        ModelSpecificMetricsHandler metricsHandler
    ) {
        this.numberOfClasses = numberOfClasses;
        this.config = config;
        this.concurrency = concurrency;
        this.random = new SplittableRandom(randomSeed.orElseGet(() -> new SplittableRandom().nextLong()));
        this.progressTracker = progressTracker;
        this.messageLogLevel = messageLogLevel;
        this.terminationFlag = terminationFlag;
        this.metricsHandler = metricsHandler;
    }

    public static MemoryEstimation memoryEstimation(
        LongUnaryOperator numberOfTrainingSamples,
        int numberOfClasses,
        MemoryRange featureDimension,
        RandomForestClassifierTrainerConfig config
    ) {
        // Since we don't expose Out-of-bag-error (yet) we do not take it into account here either.

        int minNumberOfBaggedFeatures = (int) Math.ceil(config.maxFeaturesRatio((int) featureDimension.min) * featureDimension.min);
        int maxNumberOfBaggedFeatures = (int) Math.ceil(config.maxFeaturesRatio((int) featureDimension.max) * featureDimension.max);

        return MemoryEstimations.builder("Training")
            // estimating the final forest produced
            .add(RandomForestClassifierData.memoryEstimation(numberOfTrainingSamples, config))
            .rangePerNode(
                "Impurity computation data",
                nodeCount -> config.criterion() == ClassifierImpurityCriterionType.GINI
                    ? GiniIndex.memoryEstimation(numberOfTrainingSamples.applyAsLong(nodeCount))
                    : Entropy.memoryEstimation(numberOfTrainingSamples.applyAsLong(nodeCount))
            ).perGraphDimension(
                "Decision tree training",
                (dim, concurrency) ->
                    TrainDecisionTreeTask.memoryEstimation(
                        config,
                        numberOfTrainingSamples.applyAsLong(dim.nodeCount()),
                        numberOfClasses,
                        minNumberOfBaggedFeatures,
                        config.numberOfSamplesRatio()
                    ).union(
                        TrainDecisionTreeTask.memoryEstimation(
                            config,
                            numberOfTrainingSamples.applyAsLong(dim.nodeCount()),
                            numberOfClasses,
                            maxNumberOfBaggedFeatures,
                            config.numberOfSamplesRatio()
                        )
                    ).times(concurrency)
            )
            .build();
    }

    public RandomForestClassifier train(
        Features allFeatureVectors,
        HugeIntArray allLabels,
        ReadOnlyHugeLongArray trainSet
    ) {
        Optional<HugeAtomicLongArray> maybePredictions = metricsHandler.isRequested(OUT_OF_BAG_ERROR)
            ? Optional.of(HugeAtomicLongArray.of(numberOfClasses * trainSet.size(), ParalleLongPageCreator.passThrough(concurrency)))
            : Optional.empty();

        var decisionTreeTrainConfig = DecisionTreeTrainerConfigImpl.builder()
            .maxDepth(config.maxDepth())
            .minSplitSize(config.minSplitSize())
            .build();

        int numberOfDecisionTrees = config.numberOfDecisionTrees();
        var impurityCriterion = initializeImpurityCriterion(allLabels);
        var numberOfTreesTrained = new AtomicInteger(0);

        var tasks = IntStream.range(0, numberOfDecisionTrees).mapToObj(unused ->
            new TrainDecisionTreeTask(
                maybePredictions,
                decisionTreeTrainConfig,
                config,
                random.split(),
                allFeatureVectors,
                allLabels,
                numberOfClasses,
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

        maybePredictions.ifPresent(predictions ->
        {
            outOfBagError = Optional.of(OutOfBagError.evaluate(
                trainSet,
                numberOfClasses,
                allLabels,
                concurrency,
                predictions
            ));
            metricsHandler.handle(OUT_OF_BAG_ERROR, outOfBagError());
        });

        var decisionTrees = tasks.stream().map(TrainDecisionTreeTask::trainedTree).collect(Collectors.toList());

        return new RandomForestClassifier(decisionTrees, numberOfClasses, allFeatureVectors.featureDimension());
    }

    double outOfBagError() {
        return outOfBagError.orElseThrow(() -> new IllegalAccessError("Out of bag error has not been computed."));
    }

    private ImpurityCriterion initializeImpurityCriterion(HugeIntArray allLabels) {
        switch (config.criterion()) {
            case GINI:
                return new GiniIndex(allLabels, numberOfClasses);
            case ENTROPY:
                return new Entropy(allLabels, numberOfClasses);
            default:
                throw new IllegalStateException("Invalid decision tree classifier impurity criterion.");
        }
    }

    static class TrainDecisionTreeTask implements Runnable {

        private final int numberOfClasses;
        private DecisionTreePredictor<Integer> trainedTree;
        private final Optional<HugeAtomicLongArray> maybePredictions;
        private final DecisionTreeTrainerConfig decisionTreeTrainConfig;
        private final RandomForestTrainerConfig randomForestTrainConfig;
        private final SplittableRandom random;
        private final Features allFeatureVectors;
        private final HugeIntArray allLabels;
        private final ImpurityCriterion impurityCriterion;
        private final ReadOnlyHugeLongArray trainSet;
        private final ProgressTracker progressTracker;
        private final LogLevel messageLogLevel;
        private final AtomicInteger numberOfTreesTrained;

        TrainDecisionTreeTask(
            Optional<HugeAtomicLongArray> maybePredictions,
            DecisionTreeTrainerConfig decisionTreeTrainConfig,
            RandomForestTrainerConfig randomForestTrainConfig,
            SplittableRandom random,
            Features allFeatureVectors,
            HugeIntArray allLabels,
            int numberOfClasses,
            ImpurityCriterion impurityCriterion,
            ReadOnlyHugeLongArray trainSet,
            ProgressTracker progressTracker,
            LogLevel messageLogLevel,
            AtomicInteger numberOfTreesTrained
        ) {
            this.maybePredictions = maybePredictions;
            this.decisionTreeTrainConfig = decisionTreeTrainConfig;
            this.randomForestTrainConfig = randomForestTrainConfig;
            this.random = random;
            this.allFeatureVectors = allFeatureVectors;
            this.allLabels = allLabels;
            this.numberOfClasses = numberOfClasses;
            this.impurityCriterion = impurityCriterion;
            this.trainSet = trainSet;
            this.progressTracker = progressTracker;
            this.messageLogLevel = messageLogLevel;
            this.numberOfTreesTrained = numberOfTreesTrained;
        }

        public static MemoryRange memoryEstimation(
            DecisionTreeTrainerConfig decisionTreeTrainConfig,
            long numberOfTrainingSamples,
            int numberOfClasses,
            int numberOfBaggedFeatures,
            double numberOfSamplesRatio
        ) {
            long usedNumberOfTrainingSamples = (long) Math.ceil(numberOfSamplesRatio * numberOfTrainingSamples);

            var bootstrappedDatasetEstimation = MemoryRange
                .of(HugeLongArray.memoryEstimation(usedNumberOfTrainingSamples))
                .add(MemoryUsage.sizeOfBitset(usedNumberOfTrainingSamples));

            return MemoryRange.of(sizeOfInstance(TrainDecisionTreeTask.class))
                .add(FeatureBagger.memoryEstimation(numberOfBaggedFeatures))
                .add(DecisionTreeClassifierTrainer.memoryEstimation(
                    decisionTreeTrainConfig,
                    usedNumberOfTrainingSamples,
                    numberOfClasses
                ))
                .add(bootstrappedDatasetEstimation);
        }

        public DecisionTreePredictor<Integer> trainedTree() {
            return trainedTree;
        }

        @Override
        public void run() {
            var featureBagger = new FeatureBagger(
                random,
                allFeatureVectors.featureDimension(),
                randomForestTrainConfig.maxFeaturesRatio(allFeatureVectors.featureDimension())
            );

            var decisionTree = new DecisionTreeClassifierTrainer(
                impurityCriterion,
                allFeatureVectors,
                allLabels,
                numberOfClasses,
                decisionTreeTrainConfig,
                featureBagger
            );

            var bootstrappedDataset = bootstrappedDataset();

            trainedTree = decisionTree.train(bootstrappedDataset.allVectorsIndices());

            maybePredictions.ifPresent(predictionsCache -> OutOfBagError.addPredictionsForTree(
                trainedTree,
                numberOfClasses,
                allFeatureVectors,
                trainSet,
                bootstrappedDataset.trainSetIndices(),
                predictionsCache
            ));

            progressTracker.logMessage(
                messageLogLevel,
                formatWithLocale(
                    "Trained decision tree %d out of %d",
                    numberOfTreesTrained.incrementAndGet(),
                    randomForestTrainConfig.numberOfDecisionTrees()
                )
            );
        }

        private BootstrappedDataset bootstrappedDataset() {
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

            return ImmutableBootstrappedDataset.of(
                trainSetIndices,
                allVectorsIndices
            );
        }

        @ValueClass
        interface BootstrappedDataset {
            BitSet trainSetIndices();

            ReadOnlyHugeLongArray allVectorsIndices();
        }
    }
}
