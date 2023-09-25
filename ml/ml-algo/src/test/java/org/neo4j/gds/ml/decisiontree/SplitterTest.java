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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SplitterTest {

    private static final long NUM_SAMPLES = 10;
    private static final int NUM_FEATURES = 2;

    private final HugeIntArray allLabels = HugeIntArray.newArray(NUM_SAMPLES);
    private final FeatureBagger featureBagger = new FeatureBagger(new SplittableRandom(42), NUM_FEATURES, 1.0);
    private GiniIndex giniIndexLoss;
    private Features features;

    @BeforeEach
    void setup() {
        allLabels.setAll(idx -> idx >= 5 ? 1 : 0);

        HugeObjectArray<double[]> featureVectorArray = HugeObjectArray.newArray(
            double[].class,
            NUM_SAMPLES
        );

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

        features = FeaturesFactory.wrap(featureVectorArray);

        giniIndexLoss = new GiniIndex(allLabels, 2);
    }

    private static Stream<Arguments> bestSplitParams() {
        return Stream.of(
            Arguments.of(
                HugeLongArray.of(0, 8),
                0,
                2,
                1,
                0,
                2.771244718,
                HugeLongArray.of(0),
                HugeLongArray.of(8)
            ),
            Arguments.of(
                HugeLongArray.of(0, 2, 7),
                0,
                3,
                1,
                0,
                3.678319846,
                HugeLongArray.of(0, 2),
                HugeLongArray.of(7)
            ),
            Arguments.of(
                HugeLongArray.of(3, 4, 5, 9),
                0,
                4,
                1,
                1,
                2.61995032,
                HugeLongArray.of(3, 4),
                HugeLongArray.of(5, 9)
            ),
            Arguments.of(
                HugeLongArray.of(3, 4, 9),
                0,
                3,
                1,
                0,
                6.642287351,
                HugeLongArray.of(9),
                HugeLongArray.of(3, 4)
            ),
            Arguments.of(
                HugeLongArray.of(3, 4, 9, 5),
                0,
                3,
                1,
                0,
                6.642287351,
                HugeLongArray.of(9),
                HugeLongArray.of(3, 4)
            ),
            Arguments.of(
                HugeLongArray.of(1, 9, 3, 2),
                0,
                4,
                1,
                1,
                3.31281357,
                HugeLongArray.of(1, 2, 3),
                HugeLongArray.of(9)
            ),
            Arguments.of(
                HugeLongArray.of(1, 9, 3, 2),
                0,
                4,
                2,
                0,
                3.678319846,
                HugeLongArray.of(1, 2),
                HugeLongArray.of(3, 9)
            )
        );
    }

    @ParameterizedTest
    @MethodSource("bestSplitParams")
    void shouldFindBestSplit(
        HugeLongArray groupArray,
        long startIdx,
        long size,
        int minLeafSize,
        long expectedIdx,
        double expectedValue,
        // The order of these depend on the random seed
        HugeLongArray expectedLeftChildArray,
        HugeLongArray expectedRightChildArray
    ) {
        var splitter = new Splitter(NUM_SAMPLES, giniIndexLoss, featureBagger, features, minLeafSize);
        var impurityData = giniIndexLoss.groupImpurity(groupArray, startIdx, size);
        var group = ImmutableGroup.of(groupArray, startIdx, size, impurityData);
        var split = splitter.findBestSplit(group);

        assertThat(split.index()).isEqualTo(expectedIdx);
        assertThat(split.value()).isCloseTo(expectedValue, Offset.offset(0.01D));

        Group leftChildGroup = split.groups().left();
        assertThat(leftChildGroup.size()).isEqualTo(expectedLeftChildArray.size());
        var leftChildAsList = Arrays.stream(leftChildGroup.array().toArray()).boxed().collect(Collectors.toList());
        for (long i = 0; i < expectedLeftChildArray.size(); i++) {
            assertThat((long) leftChildAsList.indexOf(expectedLeftChildArray.get(i)))
                .isLessThan(leftChildGroup.startIdx() + expectedLeftChildArray.size())
                .isGreaterThanOrEqualTo(leftChildGroup.startIdx());
        }

        Group rightChildGroup = split.groups().right();
        assertThat(rightChildGroup.size()).isEqualTo(expectedRightChildArray.size());
        var rightChildAsList = Arrays.stream(rightChildGroup.array().toArray()).boxed().collect(Collectors.toList());
        for (long i = 0; i < expectedRightChildArray.size(); i++) {
            assertThat((long)rightChildAsList.indexOf(expectedRightChildArray.get(i)))
                .isLessThan(rightChildGroup.startIdx() + expectedRightChildArray.size())
                .isGreaterThanOrEqualTo(rightChildGroup.startIdx());
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
        // Scales with training set size.
        "  1_000,  20,   40_320",
        " 10_000,  20,  400_320",
        // Changes a little with impurity data size.
        "  1_000, 100,   40_640",
    })
    void memoryEstimation(long numberOfTrainingSamples, long sizeOfImpurityData, long expectedSize) {
        long size  = Splitter.memoryEstimation(numberOfTrainingSamples, sizeOfImpurityData);
        assertThat(size).isEqualTo(expectedSize);
    }
}
