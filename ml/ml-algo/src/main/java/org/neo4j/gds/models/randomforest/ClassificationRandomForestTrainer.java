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
package org.neo4j.gds.models.randomforest;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.decisiontree.ClassificationDecisionTreeTrain;
import org.neo4j.gds.decisiontree.DecisionTreeLoss;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.decisiontree.DecisionTreeTrainConfig;
import org.neo4j.gds.decisiontree.DecisionTreeTrainConfigImpl;
import org.neo4j.gds.decisiontree.FeatureBagger;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.models.Trainer;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClassificationRandomForestTrainer<LOSS extends DecisionTreeLoss> implements Trainer {

    private final LOSS lossFunction;
    private final LocalIdMap classIdMap;
    private final RandomForestTrainConfig config;
    private final int concurrency;
    private final boolean computeOutOfBagError;
    private final SplittableRandom random;
    private Optional<Double> outOfBagError = Optional.empty();

    public ClassificationRandomForestTrainer(
        LOSS lossFunction,
        int concurrency,
        LocalIdMap classIdMap,
        RandomForestTrainConfig config,
        boolean computeOutOfBagError
    ) {
        this.lossFunction = lossFunction;
        this.classIdMap = classIdMap;
        this.config = config;
        this.concurrency = concurrency;
        this.computeOutOfBagError = computeOutOfBagError;
        this.random = config.randomSeed()
            .map(SplittableRandom::new)
            .orElseGet(SplittableRandom::new);
    }

    public ClassificationRandomForestPredictor train(Features allFeatureVectors, HugeLongArray allLabels, ReadOnlyHugeLongArray trainSet) {
        Optional<HugeAtomicLongArray> maybePredictions = computeOutOfBagError
            ? Optional.of(HugeAtomicLongArray.newArray(classIdMap.size() * trainSet.size()))
            : Optional.empty();

        var decisionTreeTrainConfig = DecisionTreeTrainConfigImpl.builder()
            .maxDepth(config.maxDepth())
            .minSplitSize(config.minSplitSize())
            .build();

        int numberOfDecisionTrees = config.numberOfDecisionTrees();
        var tasks = IntStream.range(0, numberOfDecisionTrees).mapToObj(unused ->
            new DecisionTreeTrainer<>(
                maybePredictions,
                decisionTreeTrainConfig,
                config,
                random.split(),
                allFeatureVectors,
                allLabels,
                classIdMap,
                lossFunction,
                trainSet
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

        var decisionTrees = tasks.stream().map(DecisionTreeTrainer::trainedTree).collect(Collectors.toList());

        return new ClassificationRandomForestPredictor(decisionTrees, classIdMap, allFeatureVectors.featureDimension());
    }

    double outOfBagError() {
        return outOfBagError.orElseThrow(() -> new IllegalAccessError("Out of bag error has not been computed."));
    }

    static class DecisionTreeTrainer<LOSS extends DecisionTreeLoss> implements Runnable {

        private DecisionTreePredict<Long> trainedTree;
        private final Optional<HugeAtomicLongArray> maybePredictions;
        private final DecisionTreeTrainConfig decisionTreeTrainConfig;
        private final RandomForestTrainConfig randomForestTrainConfig;
        private final SplittableRandom random;
        private final Features allFeatureVectors;
        private final HugeLongArray allLabels;
        private final LocalIdMap classIdMap;
        private final LOSS lossFunction;
        private final ReadOnlyHugeLongArray trainSet;

        DecisionTreeTrainer(
            Optional<HugeAtomicLongArray> maybePredictions,
            DecisionTreeTrainConfig decisionTreeTrainConfig,
            RandomForestTrainConfig randomForestTrainConfig,
            SplittableRandom random,
            Features allFeatureVectors,
            HugeLongArray allLabels,
            LocalIdMap classIdMap,
            LOSS lossFunction,
            ReadOnlyHugeLongArray trainSet
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
        }

        public DecisionTreePredict<Long> trainedTree() {
            return trainedTree;
        }

        @Override
        public void run() {
            var featureBagger = FeatureBagger.of(
                random,
                allFeatureVectors.featureDimension(),
                randomForestTrainConfig.featureBaggingRatio()
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
