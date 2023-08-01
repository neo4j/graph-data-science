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
package org.neo4j.gds.ml.decisiontree;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.SplittableRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DecisionTreeRegressorTest {

    private static final long NUM_SAMPLES = 10;

    private final HugeDoubleArray targets = HugeDoubleArray.newArray(NUM_SAMPLES);
    private Features features;
    private SplitMeanSquaredError mse;

    @BeforeEach
    void setup() {
        HugeObjectArray<double[]> featureVectorArray = HugeObjectArray.newArray(
            double[].class,
            NUM_SAMPLES
        );

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

        features = FeaturesFactory.wrap(featureVectorArray);

        mse = new SplitMeanSquaredError(targets);
    }

    private static Stream<Arguments> predictionWithoutSamplingParameters() {
        return Stream.of(
            Arguments.of(new double[]{8.0, 0.0}, 4.175, 1, 2),
            Arguments.of(new double[]{8.0, 0.0}, 4.175, 1, 4),
            Arguments.of(new double[]{2.0, 0.0}, 0.8916, 1, 2),
            Arguments.of(new double[]{2.0, 0.0}, 0.8916, 1, 4),
            Arguments.of(new double[]{2.0, 0.0}, 0.1699, 2, 2),
            Arguments.of(new double[]{2.0, 0.0}, 0.1699, 2, 4),
            Arguments.of(new double[]{0.0, 4.0}, 4.5, 3, 2),
            Arguments.of(new double[]{0.0, 4.0}, 4.5, 3, 4),
            Arguments.of(new double[]{5.0, 1.0}, 0.3, 4, 2),
            Arguments.of(new double[]{5.0, 1.0}, 0.225, 4, 4)
        );
    }

    @ParameterizedTest
    @MethodSource("predictionWithoutSamplingParameters")
    void shouldMakeSanePrediction(
        double[] featureVector,
        double expectedPrediction,
        int maxDepth,
        int minSplitSize
    ) {
        var decisionTreeTrainer = new DecisionTreeRegressorTrainer(
            mse,
            features,
            targets,
            DecisionTreeTrainerConfigImpl.builder()
                .maxDepth(maxDepth)
                .minSplitSize(minSplitSize)
                .build(),
            new FeatureBagger(new SplittableRandom(), featureVector.length, 1)
        );

        HugeLongArray mutableFeatureVectors = HugeLongArray.newArray(features.size());
        mutableFeatureVectors.setAll(idx -> idx);
        var featureVectors = ReadOnlyHugeLongArray.of(mutableFeatureVectors);

        var decisionTreeRegressor = decisionTreeTrainer.train(featureVectors);

        assertThat(decisionTreeRegressor.predict(featureVector)).isCloseTo(expectedPrediction, Offset.offset(0.01D));
    }

    @Test
    void indexSamplingShouldWork() {
        var decisionTreeTrainConfig = DecisionTreeTrainerConfigImpl.builder()
            .maxDepth(1)
            .minSplitSize(2)
            .build();

        HugeLongArray mutableFeatureVectors = HugeLongArray.newArray(features.size());
        mutableFeatureVectors.setAll(idx -> idx);
        var featureVectors = ReadOnlyHugeLongArray.of(mutableFeatureVectors);

        var decisionTreeTrainer = new DecisionTreeRegressorTrainer(
            mse,
            features,
            targets,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(-6938002729576536314L), features.get(0).length, 0.5D)
        );

        var featureVector = new double[]{8.0, 2.0};

        var decisionTreeRegressor = decisionTreeTrainer.train(featureVectors);
        assertThat(decisionTreeRegressor.predict(featureVector)).isCloseTo(4.175, Offset.offset(0.01D));

        decisionTreeTrainer = new DecisionTreeRegressorTrainer(
            mse,
            features,
            targets,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(1337L), featureVector.length, 0.5D) // Only one feature is used.
        );

        decisionTreeRegressor = decisionTreeTrainer.train(featureVectors);
        assertThat(decisionTreeRegressor.predict(featureVector)).isCloseTo(1.09, Offset.offset(0.01D));
    }

    @Test
    void considersSampledVectors() {
        var featureVector = new double[]{8.0, 0.0};

        var decisionTreeTrainConfig = DecisionTreeTrainerConfigImpl.builder()
            .maxDepth(1)
            .minSplitSize(2)
            .build();

        var mutableSampledVectors = HugeLongArray.newArray(1);
        mutableSampledVectors.set(0, 1);
        var sampledVectors = ReadOnlyHugeLongArray.of(mutableSampledVectors);

        var decisionTreeTrainer = new DecisionTreeRegressorTrainer(
            mse,
            features,
            targets,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(5677377167946646799L), featureVector.length, 1)
        );

        var decisionTreeRegressor = decisionTreeTrainer.train(sampledVectors);
        assertThat(decisionTreeRegressor.predict(featureVector)).isEqualTo(0.2);

        var mutableOtherSampledVectors = HugeLongArray.newArray(1);
        mutableOtherSampledVectors.set(0, features.size() - 1);
        var otherSampledVectors = ReadOnlyHugeLongArray.of(mutableOtherSampledVectors);

        decisionTreeTrainer = new DecisionTreeRegressorTrainer(
            mse,
            features,
            targets,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(321328L), featureVector.length, 1)
        );

        decisionTreeRegressor = decisionTreeTrainer.train(otherSampledVectors);
        assertThat(decisionTreeRegressor.predict(featureVector)).isEqualTo(4.5);
    }

    @Test
    void considersMinLeafSize() {
        var featureVector = new double[]{8.0, 0.0};

        var decisionTreeTrainConfig = DecisionTreeTrainerConfigImpl.builder()
            .minLeafSize(5)
            .minSplitSize(6)
            .build();

        var mutableSampledVectors = HugeLongArray.newArray(NUM_SAMPLES);
        mutableSampledVectors.setAll(i -> i);
        var sampledVectors = ReadOnlyHugeLongArray.of(mutableSampledVectors);

        var decisionTreeTrainer = new DecisionTreeRegressorTrainer(
            mse,
            features,
            targets,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(5677377167946646799L), featureVector.length, 1)
        );

        var decisionTreeRegressor = decisionTreeTrainer.train(sampledVectors);
        assertThat(decisionTreeRegressor.predict(featureVector)).isEqualTo((0.15 + 4.1 + 4.0 + 4.7 + 3.9) / 5);
    }

    @ParameterizedTest
    @CsvSource(value = {
        // Scales with training set size even if maxDepth limits tree size.
        "  6,  1_000,  40_600,    55_864",
        "  6, 10_000, 400_600,   487_864",
        // Scales with maxDepth when maxDepth is limiting tree size.
        " 20, 10_000, 400_600, 1_523_032",
    })
    void trainMemoryEstimation(int maxDepth, long numberOfTrainingSamples, long expectedMin, long expectedMax) {
        var config = DecisionTreeTrainerConfigImpl.builder()
            .maxDepth(maxDepth)
            .build();
        var range = DecisionTreeRegressorTrainer.memoryEstimation(config, numberOfTrainingSamples);

        assertThat(range.min).isEqualTo(expectedMin);
        assertThat(range.max).isEqualTo(expectedMax);
    }
}
