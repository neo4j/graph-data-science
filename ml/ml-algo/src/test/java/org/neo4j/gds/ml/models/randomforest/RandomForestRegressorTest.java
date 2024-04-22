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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryRange;

class RandomForestRegressorTest {
    private static final long NUM_SAMPLES = 10;

    private final HugeDoubleArray targets = HugeDoubleArray.newArray(NUM_SAMPLES);
    private ReadOnlyHugeLongArray trainSet;
    private Features allFeatureVectors;

    @BeforeEach
    void setup() {
        HugeLongArray mutableTrainSet = HugeLongArray.newArray(NUM_SAMPLES);
        mutableTrainSet.setAll(idx -> idx);
        trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);

        HugeObjectArray<double[]> featureVectorArray = HugeObjectArray.newArray(double[].class, NUM_SAMPLES);

        featureVectorArray.set(0, new double[]{2.771244718, 1.784783929});
        targets.set(0, 0.1);
        featureVectorArray.set(1, new double[]{1.728571309, 1.169761413});
        targets.set(1, 0.2);
        featureVectorArray.set(2, new double[]{3.678319846, 3.31281357});
        targets.set(2, 0.1);
        featureVectorArray.set(3, new double[]{6.961043357, 2.61995032});
        targets.set(3, 0.3);
        featureVectorArray.set(4, new double[]{6.999208922, 2.209014212});
        targets.set(4, 0.15);

        featureVectorArray.set(5, new double[]{7.497545867, 3.162953546});
        targets.set(5, 4.1);
        featureVectorArray.set(6, new double[]{9.00220326, 3.339047188});
        targets.set(6, 4.0);
        featureVectorArray.set(7, new double[]{7.444542326, 0.476683375});
        targets.set(7, 4.7);
        featureVectorArray.set(8, new double[]{10.12493903, 3.234550982});
        targets.set(8, 3.9);
        featureVectorArray.set(9, new double[]{6.642287351, 3.319983761});
        targets.set(9, 4.5);

        allFeatureVectors = FeaturesFactory.wrap(featureVectorArray);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingOneTree(int concurrency) {
        var randomForestTrainer = new RandomForestRegressorTrainer(
            new Concurrency(concurrency),
            RandomForestRegressorTrainerConfigImpl
                .builder()
                .maxDepth(1)
                .minSplitSize(2)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(1)
                .numberOfSamplesRatio(0.0)
                .build(),
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        var randomForestRegressor = randomForestTrainer.train(allFeatureVectors, targets, trainSet);

        var featureVector = new double[]{8.0, 0.0};

        assertThat(randomForestRegressor.predict(featureVector)).isCloseTo(4.175, Offset.offset(0.01D));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingTwentyTrees(int concurrency) {
        var randomForestTrainer = new RandomForestRegressorTrainer(
            new Concurrency(concurrency),
            RandomForestRegressorTrainerConfigImpl
                .builder()
                .maxDepth(3)
                .minSplitSize(2)
                .numberOfSamplesRatio(0.5D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(20)
                .build(),
            Optional.of(1337L),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        var randomForestRegressor = randomForestTrainer.train(allFeatureVectors, targets, trainSet);

        var featureVector = new double[]{10.0, 3.2};

        assertThat(randomForestRegressor.predict(featureVector)).isCloseTo(3.61, Offset.offset(0.01D));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void considerTrainSet(int concurrency) {
        var randomForestTrainer = new RandomForestRegressorTrainer(
            new Concurrency(concurrency),
            RandomForestRegressorTrainerConfigImpl
                .builder()
                .maxDepth(3)
                .minSplitSize(2)
                .numberOfSamplesRatio(1.0D)
                .maxFeaturesRatio(1.0D)
                .numberOfDecisionTrees(10)
                .build(),
            Optional.of(1337L),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        HugeLongArray mutableTrainSet = HugeLongArray.newArray(NUM_SAMPLES / 2);
        // Use only target ~0.2 vectors => all predictions should be around there
        mutableTrainSet.setAll(idx -> idx);
        trainSet = ReadOnlyHugeLongArray.of(mutableTrainSet);
        var randomForestRegressor = randomForestTrainer.train(allFeatureVectors, targets, trainSet);

        // target 3.9 example (see setup above)
        var featureVector = new double[]{10.12493903, 3.234550982};

        assertThat(randomForestRegressor.predict(featureVector)).isCloseTo(0.185, Offset.offset(0.01D));
    }

    @Test
    void predictOverheadMemoryEstimation() {
        var estimation = RandomForestRegressor.runtimeOverheadMemoryEstimation();

        assertMemoryRange(estimation, 16);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "     6, 100_000, 10, 1,   1, 0.1, 1.0,   4_813_650, 5_627_482",
        // Should increase fairly little with more trees if training set big.
        "    10, 100_000, 10, 1,  10, 0.1, 1.0,   4_814_370, 6_785_954",
        // Should be capped by number of training examples, despite high max depth.
        " 8_000,     500, 10, 1,   1, 0.1, 1.0,        25_210, 192_994",
        // Should increase very little when using more features for splits.
        "    10, 100_000, 10, 1,  10, 0.9, 1.0,   4_814_410, 6_786_086",
        // Should decrease a lot when sampling fewer training examples per tree.
        "    10, 100_000, 10, 1,  10, 0.1, 0.2,     964_370, 2_295_954",
        // Should almost be x4 when concurrency * 4.
        "    10, 100_000, 10, 4,  10, 0.1, 1.0, 19_254_960, 23_949_536",
    })
    void trainMemoryEstimation(
        int maxDepth,
        long numberOfTrainingSamples,
        int featureDimension,
        int concurrency,
        int numTrees,
        double maxFeaturesRatio,
        double numberOfSamplesRatio,
        long expectedMin,
        long expectedMax
    ) {
        var config = RandomForestRegressorTrainerConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .maxFeaturesRatio(maxFeaturesRatio)
            .numberOfSamplesRatio(numberOfSamplesRatio)
            .build();
        var estimator = RandomForestRegressorTrainer.memoryEstimation(
            unused -> numberOfTrainingSamples,
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
        "     6, 100_000,   1,  2,  104, 6_656",
        "     6, 100_000, 100,  2, 8_024, 663_224",
        // Max should increase with maxDepth when maxDepth limiting factor of trees' sizes.
        "    10, 100_000,   1,  2,  104, 106_496",
        // Max should scale almost inverse linearly with minSplitSize.
        "   800, 100_000,   1,  2,  104, 10_400_000",
        "   800, 100_000,   1, 10,  104, 2_080_000",
    })
    void memoryEstimation(
        int maxDepth,
        long numberOfTrainingSamples,
        int numTrees,
        int minSplitSize,
        long expectedMin,
        long expectedMax
    ) {
        var config = RandomForestRegressorTrainerConfigImpl.builder()
            .maxDepth(maxDepth)
            .numberOfDecisionTrees(numTrees)
            .minSplitSize(minSplitSize)
            .build();
        var estimator = RandomForestRegressorData.memoryEstimation(
            unused -> numberOfTrainingSamples,
            config
        );
        // Does not depend on node count, only indirectly so with the size of the training set.
        var estimation = estimator.estimate(GraphDimensions.of(10), 4).memoryUsage();

        assertMemoryRange(estimation, expectedMin, expectedMax);
    }
}
