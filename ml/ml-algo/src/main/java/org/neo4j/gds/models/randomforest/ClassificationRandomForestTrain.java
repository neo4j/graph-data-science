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
import org.neo4j.gds.decisiontree.ClassificationDecisionTreeTrain;
import org.neo4j.gds.decisiontree.DecisionTreeLoss;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.decisiontree.DecisionTreeTrainConfig;
import org.neo4j.gds.decisiontree.DecisionTreeTrainConfigImpl;
import org.neo4j.gds.decisiontree.FeatureBagger;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.models.Features;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClassificationRandomForestTrain<LOSS extends DecisionTreeLoss> {

    private final LOSS lossFunction;
    private final Features allFeatureVectors;
    private final LocalIdMap classIdMap;
    private final RandomForestTrainConfig config;
    private final int concurrency;
    private final HugeLongArray allLabels;
    private final boolean computeOutOfBagError;
    private final SplittableRandom random;

    public ClassificationRandomForestTrain(
        LOSS lossFunction,
        Features allFeatureVectors,
        int concurrency,
        LocalIdMap classIdMap,
        HugeLongArray allLabels,
        RandomForestTrainConfig config,
        boolean computeOutOfBagError
    ) {
        this.lossFunction = lossFunction;
        this.allFeatureVectors = allFeatureVectors;
        this.classIdMap = classIdMap;
        this.config = config;
        this.concurrency = concurrency;
        this.allLabels = allLabels;
        this.computeOutOfBagError = computeOutOfBagError;
        this.random = config.randomSeed()
            .map(SplittableRandom::new)
            .orElseGet(SplittableRandom::new);
    }

    public ClassificationRandomForestTrainResult train() {
        Optional<HugeAtomicLongArray> maybePredictions = computeOutOfBagError
            ? Optional.of(HugeAtomicLongArray.newArray(classIdMap.size() * allFeatureVectors.size()))
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
                lossFunction
            )
        ).collect(Collectors.toList());
        ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

        var outOfBagError = maybePredictions.map(predictions -> OutOfBagErrorMetric.evaluate(
            allFeatureVectors.size(),
            classIdMap,
            allLabels,
            concurrency,
            predictions
        ));

        var decisionTrees = tasks.stream().map(DecisionTreeTrainer::trainedTree).collect(Collectors.toList());

        return ImmutableClassificationRandomForestTrainResult.of(
            new ClassificationRandomForestPredict(decisionTrees, classIdMap),
            outOfBagError
        );
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

        DecisionTreeTrainer(
            Optional<HugeAtomicLongArray> maybePredictions,
            DecisionTreeTrainConfig decisionTreeTrainConfig,
            RandomForestTrainConfig randomForestTrainConfig,
            SplittableRandom random,
            Features allFeatureVectors,
            HugeLongArray allLabels,
            LocalIdMap classIdMap,
            LOSS lossFunction
        ) {
            this.maybePredictions = maybePredictions;
            this.decisionTreeTrainConfig = decisionTreeTrainConfig;
            this.randomForestTrainConfig = randomForestTrainConfig;
            this.random = random;
            this.allFeatureVectors = allFeatureVectors;
            this.allLabels = allLabels;
            this.classIdMap = classIdMap;
            this.lossFunction = lossFunction;
        }

        public DecisionTreePredict<Long> trainedTree() {
            return trainedTree;
        }

        @Override
        public void run() {
            var featureBagger = new FeatureBagger(
                random.split(),
                allFeatureVectors.get(0).length,
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

            trainedTree = decisionTree.train(bootstrappedDataset.indices());

            maybePredictions.ifPresent(predictionsCache -> OutOfBagErrorMetric.addPredictionsForTree(
                trainedTree,
                classIdMap,
                bootstrappedDataset.bitSet(),
                allFeatureVectors,
                predictionsCache
            ));
        }

        private BootstrappedDataset bootstrappedDataset() {
            BitSet bitSet = new BitSet(allFeatureVectors.size());
            HugeLongArray indices;

            if (Double.compare(randomForestTrainConfig.numberOfSamplesRatio(), 0.0d) == 0) {
                // 0 => no sampling but take every vector
                var allVectors = HugeLongArray.newArray(allFeatureVectors.size());
                allVectors.setAll(i -> i);
                bitSet.set(0, allFeatureVectors.size());
                indices = allVectors;
            } else {
                indices = DatasetBootstrapper.bootstrap(
                    random,
                    randomForestTrainConfig.numberOfSamplesRatio(),
                    allFeatureVectors.size(),
                    bitSet
                );
            }

            return ImmutableBootstrappedDataset.of(
                bitSet,
                indices
            );
        }

        @ValueClass
        interface BootstrappedDataset {
            BitSet bitSet();

            HugeLongArray indices();
        }
    }
}
