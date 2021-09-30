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
package org.neo4j.gds.ml.core.decisiontree;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ClassificationDecisionTreeTest {

    private static final long NUM_SAMPLES = 10;
    private static final int[] CLASSES = {0, 1};
    private final HugeIntArray allLabels = HugeIntArray.newArray(NUM_SAMPLES, AllocationTracker.empty());
    private final HugeObjectArray<double[]> allFeatures = HugeObjectArray.newArray(
        double[].class,
        NUM_SAMPLES,
        AllocationTracker.empty()
    );
    private GiniIndex giniIndexLoss;

    @BeforeEach
    void setup() {
        allLabels.set(0, 0);
        allLabels.set(1, 0);
        allLabels.set(2, 0);
        allLabels.set(3, 0);
        allLabels.set(4, 0);
        allLabels.set(5, 1);
        allLabels.set(6, 1);
        allLabels.set(7, 1);
        allLabels.set(8, 1);
        allLabels.set(9, 1);

        allFeatures.set(0, new double[]{2.771244718, 1.784783929});
        allFeatures.set(1, new double[]{1.728571309, 1.169761413});
        allFeatures.set(2, new double[]{3.678319846, 3.31281357});
        allFeatures.set(3, new double[]{6.961043357, 2.61995032});
        allFeatures.set(4, new double[]{6.999208922, 2.209014212});
        allFeatures.set(5, new double[]{7.497545867, 3.162953546});
        allFeatures.set(6, new double[]{9.00220326, 3.339047188});
        allFeatures.set(7, new double[]{7.444542326, 0.476683375});
        allFeatures.set(8, new double[]{10.12493903, 3.234550982});
        allFeatures.set(9, new double[]{6.642287351, 3.319983761});

        giniIndexLoss = new GiniIndex(CLASSES, allLabels);
    }

    private static Stream<Arguments> sanePredictionParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                Arguments.of(new double[]{8.0, 0.0}, 1, 1),
                Arguments.of(new double[]{3.0, 0.0}, 0, 1),
                Arguments.of(new double[]{0.0, 4.0}, 1, 2),
                Arguments.of(new double[]{3.0, 0.0}, 0, 100),
                Arguments.of(new double[]{0.0, 4.0}, 1, 100)
            ),
            () -> Stream.of(Arguments.of(1), Arguments.of(3))
        );
    }

    @ParameterizedTest
    @MethodSource("sanePredictionParameters")
    void shouldMakeSanePrediction(double[] features, int expectedPrediction, int maxDepth, int minSize) {
        var decisionTree = new ClassificationDecisionTreeTrain<>(
            AllocationTracker.empty(),
            giniIndexLoss,
            allFeatures,
            maxDepth,
            minSize,
            CLASSES,
            allLabels
        );

        var decisionTreePredict = decisionTree.train();

        assertThat(decisionTreePredict.predict(features)).isEqualTo(expectedPrediction);
    }
}
