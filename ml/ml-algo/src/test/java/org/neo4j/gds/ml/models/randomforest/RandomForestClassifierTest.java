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
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;

class RandomForestClassifierTest {
    private static final long NUM_SAMPLES = 10;
    private final HugeIntArray allLabels = HugeIntArray.newArray(NUM_SAMPLES);
    private ReadOnlyHugeLongArray trainSet;
    private Features allFeatureVectors;
    private int numberOfClasses;

    @BeforeEach
    void setup() {
        allLabels.setAll(idx -> idx >= 5 ? 1 : 0);
        numberOfClasses = 2;

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
        RandomForestClassifier randomForestPredictor
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

        return maxClassIdx;
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingOneTree(int concurrency) {
        var randomForestTrainer = new RandomForestClassifierTrainer(
            concurrency,
            numberOfClasses,
            RandomForestClassifierTrainerConfigImpl.builder()
                .maxDepth(1)
                .minSplitSize(2)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(1)
                .build(),
            Optional.of(42L),
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            ModelSpecificMetricsHandler.NOOP
        );

        var randomForestPredictor = randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        var featureVector = new double[]{8.0, 0.0};

        assertThrows(
            IllegalAccessError.class,
            randomForestTrainer::outOfBagError
        );
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(1);
        assertThat(randomForestPredictor.predictProbabilities(featureVector)).containsExactly(0.0, 1.0);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingTwentyTrees(int concurrency) {
        var randomForestTrainer = new RandomForestClassifierTrainer(
            concurrency,
            numberOfClasses,
            RandomForestClassifierTrainerConfigImpl.builder()
                .maxDepth(2)
                .minSplitSize(2)
                .numberOfSamplesRatio(0.5D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(20)
                .build(),
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            ModelSpecificMetricsHandler.NOOP
        );

        var randomForestPredictor = randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        var featureVector = new double[]{8.0, 3.2};

        assertThrows(
            IllegalAccessError.class,
            randomForestTrainer::outOfBagError
        );
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(1);
        assertThat(randomForestPredictor.predictProbabilities(featureVector)).containsExactly(0.15, 0.85);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingTwentyTreesAndEntropyLoss(int concurrency) {
        var randomForestTrainer = new RandomForestClassifierTrainer(
            concurrency,
            numberOfClasses,
            RandomForestClassifierTrainerConfigImpl.builder()
                .criterion("entropy")
                .maxDepth(2)
                .minSplitSize(2)
                .numberOfSamplesRatio(0.5D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(20)
                .build(),
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            ModelSpecificMetricsHandler.NOOP
        );

        var randomForestPredictor = randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        var featureVector = new double[]{8.0, 3.2};

        assertThrows(
            IllegalAccessError.class,
            randomForestTrainer::outOfBagError
        );
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(1);
        assertThat(randomForestPredictor.predictProbabilities(featureVector)).containsExactly(0.15, 0.85);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldMakeSaneErrorEstimation(int concurrency) {
        var randomForestTrainer = new RandomForestClassifierTrainer(
            concurrency,
            numberOfClasses,
            RandomForestClassifierTrainerConfigImpl
                .builder()
                .maxDepth(2)
                .minSplitSize(2)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(20)
                .build(),
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            ModelSpecificMetricsHandler.ignoringResult(List.of(OUT_OF_BAG_ERROR))
        );

        randomForestTrainer.train(allFeatureVectors, allLabels, trainSet);

        assertThat(randomForestTrainer.outOfBagError()).isCloseTo(0.1, Offset.offset(0.000001D));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void considerTrainSet(int concurrency) {
        var randomForestTrainer = new RandomForestClassifierTrainer(
            concurrency,
            numberOfClasses,
            RandomForestClassifierTrainerConfigImpl
                .builder()
                .maxDepth(2)
                .minSplitSize(2)
                .numberOfSamplesRatio(0.5D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(5)
                .build(),
            Optional.of(1337L),
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO,
            TerminationFlag.RUNNING_TRUE,
            ModelSpecificMetricsHandler.NOOP
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
        assertThat(predictLabel(featureVector, randomForestPredictor)).isEqualTo(0);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  10,   168,   168",
        " 100, 1_248, 1_248"
    })
    void predictOverheadMemoryEstimation(
        int numberOfClasses,
        long expectedMin,
        long expectedMax
    ) {
        var estimation = RandomForestClassifier.runtimeOverheadMemoryEstimation(numberOfClasses);

        assertMemoryRange(estimation, expectedMin, expectedMax);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "     6, 100_000,  10, 10, 1,   1, 0.1, 1.0,   5_214_106, 6_026_930",
        // Should increase fairly little with more trees if training set big.
        "    10, 100_000,  10, 10, 1,  10, 0.1, 1.0,   5_214_754, 7_096_314",
        // Should be capped by number of training examples, despite high max depth.
        " 8_000,     500,  10, 10, 1,   1, 0.1, 1.0,        27_666, 187_466",
        // Should increase very little when having more classes.
        "    10, 100_000, 100, 10, 1,  10, 0.1, 1.0,   5_218_354, 7_099_914",
        // Should increase very little when using more features for splits.
        "    10, 100_000, 100, 10, 1,  10, 0.9, 1.0,   5_218_394, 7_100_046",
        // Should decrease a lot when sampling fewer training examples per tree.
        "    10, 100_000, 100, 10, 1,  10, 0.1, 0.2,   1_368_354, 2_609_914",
        // Should almost be x4 when concurrency * 4.
        "    10, 100_000, 100, 10, 4,  10, 0.1, 1.0, 19_670_992, 24_250_992",
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
        var config = RandomForestClassifierTrainerConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .maxFeaturesRatio(maxFeaturesRatio)
            .numberOfSamplesRatio(numberOfSamplesRatio)
            .build();
        var estimator = RandomForestClassifierTrainer.memoryEstimation(
            unused -> numberOfTrainingSamples,
            numberOfClasses,
            MemoryRange.of(featureDimension),
            config
        );
        // Does not depend on node count, only indirectly so with the size of the training set.
        var estimation = estimator.estimate(GraphDimensions.of(10), concurrency).memoryUsage();

        assertMemoryRange(estimation, expectedMin, expectedMax);
    }

    @ParameterizedTest
    @CsvSource(value = {
        // Max should almost scale linearly with numberOfDecisionTrees.
        "     6, 100_000,   1,  2,    96, 6_144",
        "     6, 100_000, 100,  2,  7_224, 612_024",
        // Max should increase with maxDepth when maxDepth limiting factor of trees' sizes.
        "    10, 100_000,   1,  2,    96, 98_304",
        // Max should scale almost inverse linearly with minSplitSize.
        "   800, 100_000,   1,  2,    96, 9_600_000",
        "   800, 100_000,   1, 10,    96, 1_920_000",
    })
    void memoryEstimation(
        int maxDepth,
        long numberOfTrainingSamples,
        int numTrees,
        int minSplitSize,
        long expectedMin,
        long expectedMax
    ) {
        var config = RandomForestTrainerConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .minSplitSize(minSplitSize)
            .build();
        var estimator = RandomForestClassifierData.memoryEstimation(
            unused -> numberOfTrainingSamples,
            config
        );
        // Does not depend on node count, only indirectly so with the size of the training set.
        var estimation = estimator.estimate(GraphDimensions.of(10), 4).memoryUsage();

        assertMemoryRange(estimation, expectedMin, expectedMax);
    }
}
