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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClassificationRandomForestTest {
    private static final long NUM_SAMPLES = 10;
    private static final LocalIdMap CLASS_MAPPING = LocalIdMap.of(1337, 42);

    private final HugeLongArray allLabels = HugeLongArray.newArray(NUM_SAMPLES);
    private ReadOnlyHugeLongArray trainSet;
    private Features allFeatureVectors;

    @BeforeEach
    void setup() {
        allLabels.setAll(idx -> idx >= 5 ? 42 : 1337);

        HugeLongArray mutableTrainSet = HugeLongArray.newArray(NUM_SAMPLES);
        mutableTrainSet.setAll(idx -> idx);
        trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        HugeObjectArray<double[]> featureVectorArray = HugeObjectArray.newArray(double[].class, NUM_SAMPLES);

        // Class 1337 feature vectors.
        featureVectorArray.set(0, new double[]{2.771244718, 1.784783929});
        featureVectorArray.set(1, new double[]{1.728571309, 1.169761413});
        featureVectorArray.set(2, new double[]{3.678319846, 3.31281357});
        featureVectorArray.set(3, new double[]{6.961043357, 2.61995032});
        featureVectorArray.set(4, new double[]{6.999208922, 2.209014212});

        // Class 42 feature vectors.
        featureVectorArray.set(5, new double[]{7.497545867, 3.162953546});
        featureVectorArray.set(6, new double[]{9.00220326, 3.339047188});
        featureVectorArray.set(7, new double[]{7.444542326, 0.476683375});
        featureVectorArray.set(8, new double[]{10.12493903, 3.234550982});
        featureVectorArray.set(9, new double[]{6.642287351, 3.319983761});

        allFeatureVectors = FeaturesFactory.wrap(featureVectorArray);
    }

    long predictLabel(
        final double[] features,
        ClassificationRandomForestPredictor randomForestPredictor
    ) {
        final int[] predictionsPerClass = randomForestPredictor.gatherTreePredictions(features);

        int max = -1;
        int maxClassIdx = 0;

        for (int i = 0; i < predictionsPerClass.length; i++) {
            var numPredictions = predictionsPerClass[i];

            if (numPredictions <= max) continue;

            max = numPredictions;
            maxClassIdx = i;
        }

        return ClassificationRandomForestTest.CLASS_MAPPING.toOriginal(maxClassIdx);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingOneTree(int concurrency) {
        var randomForestTrainer = new ClassificationRandomForestTrainer(
            concurrency,
            CLASS_MAPPING,
            RandomForestTrainConfigImpl
                .builder()
                .maxDepth(1)
                .minSplitSize(2)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(1)
                .build(),
            false,
            Optional.of(42L),
            ProgressTracker.NULL_TRACKER
        );

        var randomForestPredictor = randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        var featureVector = new double[]{8.0, 0.0};

        assertThrows(
            IllegalAccessError.class,
            randomForestTrainer::outOfBagError
        );
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(42);
        assertThat(randomForestPredictor.predictProbabilities(featureVector)).containsExactly(0.0, 1.0);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingTwentyTrees(int concurrency) {
        var randomForestTrainer = new ClassificationRandomForestTrainer(
            concurrency,
            CLASS_MAPPING,
            RandomForestTrainConfigImpl
                .builder()
                .maxDepth(2)
                .minSplitSize(2)
                .numberOfSamplesRatio(0.5D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(20)
                .build(),
            false,
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER
        );

        var randomForestPredictor = randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        var featureVector = new double[]{8.0, 3.2};

        assertThrows(
            IllegalAccessError.class,
            randomForestTrainer::outOfBagError
        );
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(42);
        assertThat(randomForestPredictor.predictProbabilities(featureVector)).containsExactly(0.4, 0.6);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldMakeSaneErrorEstimation(int concurrency) {
        var randomForestTrainer = new ClassificationRandomForestTrainer(
            concurrency,
            CLASS_MAPPING,
            RandomForestTrainConfigImpl
                .builder()
                .maxDepth(2)
                .minSplitSize(2)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(20)
                .build(),
            true,
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER
        );

        randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        assertThat(randomForestTrainer.outOfBagError()).isCloseTo(0.2, Offset.offset(0.000001D));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void considerTrainSet(int concurrency) {
        var randomForestTrainer = new ClassificationRandomForestTrainer(
            concurrency,
            CLASS_MAPPING,
            RandomForestTrainConfigImpl
                .builder()
                .maxDepth(2)
                .minSplitSize(2)
                .numberOfSamplesRatio(0.5D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(5)
                .build(),
            false,
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER
        );

        HugeLongArray mutableTrainSet = HugeLongArray.newArray(NUM_SAMPLES / 2);
        // Use only class 1337 vectors => all predictions should be 1337
        mutableTrainSet.setAll(idx -> idx);
        trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);
        var randomForestPredictor = randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        // 42 example (see setup above)
        var featureVector = new double[]{7.444542326, 0.476683375};

        assertThrows(
            IllegalAccessError.class,
            randomForestTrainer::outOfBagError
        );
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(1337);
    }

    @ParameterizedTest
    @CsvSource(value = {
        // Max should almost scale linearly with numberOfDecisionTrees.
        "     6, 100_000,  10,   1,  2,   248,      5_288",
        "     6, 100_000,  10, 100,  2, 5_792,    509_792",
        // Max should increase with maxDepth when maxDepth limiting factor of trees' sizes.
        "    10, 100_000,  10,   1,  2,   248,     82_088",
        // Min should increase with class sized arrays caching.
        "    10, 100_000, 100,   1,  2,  1_328,    83_168",
        // Max should scale almost inverse linearly with minSplitSize.
        "   800, 100_000, 100,   1,  2,  1_328, 8_001_248",
        "   800, 100_000, 100,   1, 10,  1_328, 1_601_248",
    })
    void predictMemoryEstimation(
        int maxDepth,
        long numberOfTrainingSamples,
        int numberOfClasses,
        int numTrees,
        int minSplitSize,
        long expectedMin,
        long expectedMax
    ) {
        var config = RandomForestTrainConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .minSplitSize(minSplitSize)
            .build();
        var estimation = ClassificationRandomForestPredictor.memoryEstimation(
            numberOfTrainingSamples,
            numberOfClasses,
            config
        );

        assertThat(estimation.min).isEqualTo(expectedMin);
        assertThat(estimation.max).isEqualTo(expectedMax);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "     6, 100_000,  10, 10, 1,   1, 0.1, 1.0, 4_501_194,   5_312_002",
        // Should increase fairly little with more trees if training set big.
        "    10, 100_000,  10, 10, 1,  10, 0.1, 1.0, 4_501_698,   6_203_210",
        // Should be capped by number of training examples, despite high max depth.
        " 8_000,     500,  10, 10, 1,   1, 0.1, 1.0,    23_694,   1_127_526",
        // Should increase very little when having more classes.
        "    10, 100_000, 100, 10, 1,  10, 0.1, 1.0, 4_503_498,   6_205_010",
        // Should increase very little when using more features for splits.
        "    10, 100_000, 100, 10, 1,  10, 0.9, 1.0, 4_503_570,   6_205_174",
        // Should decrease a lot when sampling fewer training examples per tree.
        "    10, 100_000, 100, 10, 1,  10, 0.1, 0.2, 1_223_498,   2_285_010",
        // Should almost be x4 when concurrency * 4.
        "    10, 100_000, 100, 10, 4,  10, 0.1, 1.0, 16_808_184, 21_159_032",
    })
    void trainMemoryEstimation(
        int maxDepth,
        long numberOfTrainingSamples,
        int numberOfClasses,
        int featureDimension,
        int concurrency,
        int numTrees,
        double maxFeaturesRatio,
        double numberOfSamplesRatio,
        long expectedMin,
        long expectedMax
    ) {
        var config = RandomForestTrainConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .maxFeaturesRatio(maxFeaturesRatio)
            .numberOfSamplesRatio(numberOfSamplesRatio)
            .build();
        var estimator = ClassificationRandomForestTrainer.memoryEstimation(
            numberOfTrainingSamples,
            numberOfClasses,
            featureDimension,
            config
        );
        // Does not depend on node count, only indirectly so with the size of the training set.
        var estimation = estimator.estimate(GraphDimensions.of(10), concurrency).memoryUsage();

        assertThat(estimation.min).isEqualTo(expectedMin);
        assertThat(estimation.max).isEqualTo(expectedMax);
    }
}
