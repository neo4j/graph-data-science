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
package org.neo4j.gds.ml.core.randomforest;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.decisiontree.GiniIndex;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ClassificationRandomForestTest {
    private static final long NUM_SAMPLES = 10;
    private static final int[] CLASSES = {1337, 42};
    private static final Map<Integer, Integer> CLASS_TO_IDX = Map.of(
        1337, 0,
        42, 1
    );

    private final HugeIntArray allLabels = HugeIntArray.newArray(NUM_SAMPLES, AllocationTracker.empty());
    private final HugeObjectArray<double[]> allFeatureVectors = HugeObjectArray.newArray(
        double[].class,
        NUM_SAMPLES,
        AllocationTracker.empty()
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

        giniIndexLoss = new GiniIndex(CLASSES, allLabels, CLASS_TO_IDX);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingOneTree(int concurrency) {
        var randomForestTrain = new ClassificationRandomForestTrain(
            AllocationTracker.empty(),
            giniIndexLoss,
            allFeatureVectors,
            1,
            1,
            0.0D,
            0.0D,
            Optional.empty(),
            1,
            concurrency,
            CLASSES,
            allLabels
        );

        var randomForestPredict = randomForestTrain.train().predictor();

        var features = new double[]{8.0, 0.0};
        assertThat(randomForestPredict.predict(features)).isEqualTo(42);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void usingTwentyTrees(int concurrency) {
        var randomForestTrain = new ClassificationRandomForestTrain(
            AllocationTracker.empty(),
            giniIndexLoss,
            allFeatureVectors,
            2,
            1,
            0.0D,
            0.5D,
            Optional.of(1337L),
            20,
            concurrency,
            CLASSES,
            allLabels
        );

        var randomForestPredict = randomForestTrain.train().predictor();

        var features = new double[]{8.0, 3.2};
        assertThat(randomForestPredict.predict(features)).isEqualTo(42);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldMakeSaneErrorEstimation(int concurrency) {
        var randomForestTrain = new ClassificationRandomForestTrain(
            AllocationTracker.empty(),
            giniIndexLoss,
            allFeatureVectors,
            2,
            1,
            0.0D,
            1.0D,
            Optional.of(1337L),
            20,
            concurrency,
            CLASSES,
            allLabels
        );

        var trainResult = randomForestTrain.train();
        var randomForestPredict = trainResult.predictor();
        var bootstrappedDatasets = trainResult.bootstrappedDatasets();

        assertThat(randomForestPredict.outOfBagError(bootstrappedDatasets, allFeatureVectors, allLabels))
            .isCloseTo(0.2, Offset.offset(0.000001D));
    }
}
