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
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.decisiontree.ClassificationDecisionTreeTrain;
import org.neo4j.gds.ml.decisiontree.DecisionTreeLoss;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredict;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainConfig;
import org.neo4j.gds.ml.decisiontree.DecisionTreeTrainConfigImpl;
import org.neo4j.gds.ml.decisiontree.FeatureBagger;
import org.neo4j.gds.ml.decisiontree.GiniIndex;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.Trainer;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class ClassificationRandomForestTrainer implements Trainer {

    private final LocalIdMap classIdMap;
    private final RandomForestTrainConfig config;
    private final int concurrency;
    private final boolean computeOutOfBagError;
    private final SplittableRandom random;
    private final ProgressTracker progressTracker;
    private Optional<Double> outOfBagError = Optional.empty();

    public ClassificationRandomForestTrainer(
        int concurrency,
        LocalIdMap classIdMap,
        RandomForestTrainConfig config,
        boolean computeOutOfBagError,
        Optional<Long> randomSeed,
        ProgressTracker progressTracker
    ) {
        this.classIdMap = classIdMap;
        this.config = config;
        this.concurrency = concurrency;
        this.computeOutOfBagError = computeOutOfBagError;
        this.random = new SplittableRandom(randomSeed.orElseGet(() -> new SplittableRandom().nextLong()));
        this.progressTracker = progressTracker;
    }

    public static MemoryEstimation memoryEstimation(
        long numberOfTrainingSamples,
        int numberOfClasses,
        int featureDimension,
        RandomForestTrainConfig config
    ) {
        // Since we don't expose Out-of-bag-error (yet) we do not take it into account here either.

        int numberOfBaggedFeatures = (int) Math.ceil(config.maxFeaturesRatio(featureDimension) * featureDimension);

        return MemoryEstimations.builder("Training", ClassificationRandomForestTrainer.class)
            .add(MemoryEstimations.of(
                "Classifier",
                ClassificationRandomForestPredictor.memoryEstimation(numberOfTrainingSamples, numberOfClasses, config)
            ))
            .add(MemoryEstimations.of("GiniIndex Loss", GiniIndex.memoryEstimation(numberOfTrainingSamples)))
            .perThread(
                "Decision tree training",
                TrainDecisionTreeTask.memoryEstimation(
                    config.maxDepth(),
                    numberOfTrainingSamples,
                    numberOfClasses,
                    numberOfBaggedFeatures,
                    config.numberOfSamplesRatio()
                )
            )
            .build();
    }

    public ClassificationRandomForestPredictor train(
        Features allFeatureVectors,
        HugeLongArray allLabels,
        ReadOnlyHugeLongArray trainSet
    ) {
        Optional<HugeAtomicLongArray> maybePredictions = computeOutOfBagError
            ? Optional.of(HugeAtomicLongArray.newArray(classIdMap.size() * trainSet.size()))
            : Optional.empty();

        var decisionTreeTrainConfig = DecisionTreeTrainConfigImpl.builder()
            .maxDepth(config.maxDepth())
            .minSplitSize(config.minSplitSize())
            .build();

        int numberOfDecisionTrees = config.numberOfDecisionTrees();
        var lossFunction = GiniIndex.fromOriginalLabels(allLabels, classIdMap);

        progressTracker.setVolume(numberOfDecisionTrees);
        var numberOfTreesTrained = new AtomicInteger(0);

        var tasks = IntStream.range(0, numberOfDecisionTrees).mapToObj(unused ->
            new TrainDecisionTreeTask<>(
                maybePredictions,
                decisionTreeTrainConfig,
                config,
                random.split(),
                allFeatureVectors,
                allLabels,
                classIdMap,
                lossFunction,
                trainSet,
                progressTracker,
                numberOfTreesTrained
            )
        ).collect(Collectors.toList());
        ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

        outOfBagError = maybePredictions.map(predictions -> OutOfBagErrorMetric.evaluate(
            trainSet,
            classIdMap,
            allLabels,
            concurrency,
            predictions
        ));

        var decisionTrees = tasks.stream().map(TrainDecisionTreeTask::trainedTree).collect(Collectors.toList());

        return new ClassificationRandomForestPredictor(decisionTrees, classIdMap, allFeatureVectors.featureDimension());
    }

    double outOfBagError() {
        return outOfBagError.orElseThrow(() -> new IllegalAccessError("Out of bag error has not been computed."));
    }

    static class TrainDecisionTreeTask<LOSS extends DecisionTreeLoss> implements Runnable {

        private DecisionTreePredict<Integer> trainedTree;
        private final Optional<HugeAtomicLongArray> maybePredictions;
        private final DecisionTreeTrainConfig decisionTreeTrainConfig;
        private final RandomForestTrainConfig randomForestTrainConfig;
        private final SplittableRandom random;
        private final Features allFeatureVectors;
        private final HugeLongArray allLabels;
        private final LocalIdMap classIdMap;
        private final LOSS lossFunction;
        private final ReadOnlyHugeLongArray trainSet;
        private final ProgressTracker progressTracker;
        private final AtomicInteger numberOfTreesTrained;

        TrainDecisionTreeTask(
            Optional<HugeAtomicLongArray> maybePredictions,
            DecisionTreeTrainConfig decisionTreeTrainConfig,
            RandomForestTrainConfig randomForestTrainConfig,
            SplittableRandom random,
            Features allFeatureVectors,
            HugeLongArray allLabels,
            LocalIdMap classIdMap,
            LOSS lossFunction,
            ReadOnlyHugeLongArray trainSet,
            ProgressTracker progressTracker,
            AtomicInteger numberOfTreesTrained
        ) {
            this.maybePredictions = maybePredictions;
            this.decisionTreeTrainConfig = decisionTreeTrainConfig;
            this.randomForestTrainConfig = randomForestTrainConfig;
            this.random = random;
            this.allFeatureVectors = allFeatureVectors;
            this.allLabels = allLabels;
            this.classIdMap = classIdMap;
            this.lossFunction = lossFunction;
            this.trainSet = trainSet;
            this.progressTracker = progressTracker;
            this.numberOfTreesTrained = numberOfTreesTrained;
        }

        public static MemoryRange memoryEstimation(
            int maxDepth,
            long numberOfTrainingSamples,
            int numberOfClasses,
            int numberOfBaggedFeatures,
            double numberOfSamplesRatio
        ) {
            long usedNumberOfTrainingSamples = (long) Math.ceil(numberOfSamplesRatio * numberOfTrainingSamples);

            var bootstrappedDatasetEstimation = MemoryRange
                .of(HugeLongArray.memoryEstimation(usedNumberOfTrainingSamples))
                .add(sizeOfInstance(BitSet.class))
                .add(usedNumberOfTrainingSamples);

            return MemoryRange.of(sizeOfInstance(TrainDecisionTreeTask.class))
                .add(FeatureBagger.memoryEstimation(numberOfBaggedFeatures))
                .add(ClassificationDecisionTreeTrain.memoryEstimation(
                    maxDepth,
                    usedNumberOfTrainingSamples,
                    numberOfBaggedFeatures,
                    numberOfClasses
                ))
                .add(bootstrappedDatasetEstimation);
        }

        public DecisionTreePredict<Integer> trainedTree() {
            return trainedTree;
        }

        @Override
        public void run() {
            var featureBagger = new FeatureBagger(
                random,
                allFeatureVectors.featureDimension(),
                randomForestTrainConfig.maxFeaturesRatio(allFeatureVectors.featureDimension())
            );

            var decisionTree = new ClassificationDecisionTreeTrain<>(
                lossFunction,
                allFeatureVectors,
                allLabels,
                classIdMap,
                decisionTreeTrainConfig,
                featureBagger
            );

            var bootstrappedDataset = bootstrappedDataset();

            trainedTree = decisionTree.train(bootstrappedDataset.allVectorsIndices());

            maybePredictions.ifPresent(predictionsCache -> OutOfBagErrorMetric.addPredictionsForTree(
                trainedTree,
                classIdMap,
                allFeatureVectors,
                trainSet,
                bootstrappedDataset.trainSetIndices(),
                predictionsCache
            ));

            progressTracker.logProgress(
                1,
                formatWithLocale(
                    ":: trained decision tree %d out of %d",
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
