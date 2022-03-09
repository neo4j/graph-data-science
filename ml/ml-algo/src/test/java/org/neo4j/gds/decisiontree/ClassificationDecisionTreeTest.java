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
package org.neo4j.gds.decisiontree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.SplittableRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ClassificationDecisionTreeTest {

    private static final long NUM_SAMPLES = 10;
    private static final LocalIdMap CLASS_MAPPING = LocalIdMap.of(1337, 42);

    private final HugeLongArray allLabels = HugeLongArray.newArray(NUM_SAMPLES);
    private final HugeObjectArray<double[]> allFeatureVectors = HugeObjectArray.newArray(
        double[].class,
        NUM_SAMPLES
    );
    private GiniIndex giniIndexLoss;

    @BeforeEach
    void setup() {
        allLabels.setAll(idx -> idx >= 5 ? 42 : 1337);

        // Class 1337 feature vectors.
        allFeatureVectors.set(0, new double[]{2.771244718, 1.784783929});
        allFeatureVectors.set(1, new double[]{1.728571309, 1.169761413});
        allFeatureVectors.set(2, new double[]{3.678319846, 3.31281357});
        allFeatureVectors.set(3, new double[]{6.961043357, 2.61995032});
        allFeatureVectors.set(4, new double[]{6.999208922, 2.209014212});

        // Class 42 feature vectors.
        allFeatureVectors.set(5, new double[]{7.497545867, 3.162953546});
        allFeatureVectors.set(6, new double[]{9.00220326, 3.339047188});
        allFeatureVectors.set(7, new double[]{7.444542326, 0.476683375});
        allFeatureVectors.set(8, new double[]{10.12493903, 3.234550982});
        allFeatureVectors.set(9, new double[]{6.642287351, 3.319983761});

        giniIndexLoss = new GiniIndex(allLabels, CLASS_MAPPING);
    }

    private static Stream<Arguments> predictionWithoutSamplingParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                Arguments.of(new double[]{8.0, 0.0}, 42, 1),
                Arguments.of(new double[]{3.0, 0.0}, 1337, 1),
                Arguments.of(new double[]{0.0, 4.0}, 42, 2),
                Arguments.of(new double[]{3.0, 0.0}, 1337, 100),
                Arguments.of(new double[]{0.0, 4.0}, 42, 100)
            ),
            () -> Stream.of(Arguments.of(1), Arguments.of(3))
        );
    }

    @ParameterizedTest
    @MethodSource("predictionWithoutSamplingParameters")
    void shouldMakeSanePrediction(
        double[] features,
        int expectedPrediction,
        int maxDepth,
        int minSize
    ) {
        var decisionTree = new ClassificationDecisionTreeTrain<>(
            giniIndexLoss,
            allFeatureVectors,
            allLabels,
            CLASS_MAPPING,
            DecisionTreeTrainConfigImpl.builder()
                .maxDepth(maxDepth)
                .minSplitSize(minSize)
                .build(),
            new FeatureBagger(new SplittableRandom(), features.length, 1)
        );

        HugeLongArray activeFeatureVectors = HugeLongArray.newArray(allFeatureVectors.size());
        activeFeatureVectors.setAll(idx -> idx);

        var decisionTreePredict = decisionTree.train(activeFeatureVectors);

        assertThat(decisionTreePredict.predict(features)).isEqualTo(expectedPrediction);
    }

    @Test
    void indexSamplingShouldWork() {
        var decisionTreeTrainConfig = DecisionTreeTrainConfigImpl.builder()
            .maxDepth(1)
            .minSplitSize(1)
            .build();

        HugeLongArray activeFeatureVectors = HugeLongArray.newArray(allFeatureVectors.size());
        activeFeatureVectors.setAll(idx -> idx);

        var decisionTree = new ClassificationDecisionTreeTrain<>(
            giniIndexLoss,
            allFeatureVectors,
            allLabels,
            CLASS_MAPPING,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(-6938002729576536314L), allFeatureVectors.get(0).length, 0.5D)
        );

        var features = new double[]{8.0, 0.0};

        var decisionTreePredict = decisionTree.train(activeFeatureVectors);
        assertThat(decisionTreePredict.predict(features)).isEqualTo(42);

        decisionTree = new ClassificationDecisionTreeTrain<>(
            giniIndexLoss,
            allFeatureVectors,
            allLabels,
            CLASS_MAPPING,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(1337L), features.length, 0.5D) // Only one feature is used.
        );

        decisionTreePredict = decisionTree.train(activeFeatureVectors);
        assertThat(decisionTreePredict.predict(features)).isEqualTo(1337);
    }

    @Test
    void considersSampledVectors() {
        var features = new double[]{8.0, 0.0};

        var decisionTreeTrainConfig = DecisionTreeTrainConfigImpl.builder()
            .maxDepth(1)
            .minSplitSize(1)
            .build();

        var sampledVectors = HugeLongArray.newArray(1);
        sampledVectors.set(0, 1);

        var decisionTree = new ClassificationDecisionTreeTrain<>(
            giniIndexLoss,
            allFeatureVectors,
            allLabels,
            CLASS_MAPPING,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(5677377167946646799L), features.length, 1)
        );

        var decisionTreePredict = decisionTree.train(sampledVectors);
        assertThat(decisionTreePredict.predict(features)).isEqualTo(1337L);

        var otherSampledVectors = HugeLongArray.newArray(1);
        otherSampledVectors.set(0, allFeatureVectors.size() - 1);

        decisionTree = new ClassificationDecisionTreeTrain<>(
            giniIndexLoss,
            allFeatureVectors,
            allLabels,
            CLASS_MAPPING,
            decisionTreeTrainConfig,
            new FeatureBagger(new SplittableRandom(321328L), features.length, 1)
        );

        decisionTreePredict = decisionTree.train(otherSampledVectors);
        assertThat(decisionTreePredict.predict(features)).isEqualTo(42L);
    }
}
