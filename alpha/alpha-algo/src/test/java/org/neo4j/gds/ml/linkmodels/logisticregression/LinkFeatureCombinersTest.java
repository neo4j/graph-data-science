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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.math.L2Norm;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LinkFeatureCombinersTest {

    @ParameterizedTest
    @MethodSource("l2InputArrays")
    void shouldCombineArraysUsingL2(double[] sourceArray, double[] targetArray, double[] expectedCombined) {
        var combined = LinkFeatureCombiners.L2.combine(sourceArray, targetArray);
        assertThat(combined).containsExactly(expectedCombined);
    }

    @ParameterizedTest
    @MethodSource("hadamardInputArrays")
    void shouldCombineArraysUsingHADAMARD(double[] sourceArray, double[] targetArray, double[] expectedCombined) {
        var combined = LinkFeatureCombiners.HADAMARD.combine(sourceArray, targetArray);
        assertThat(combined).containsExactly(expectedCombined);
    }

    @ParameterizedTest
    @MethodSource("cosineInputArrays")
    void shouldCombineArraysUsingCOSINE(double[] sourceArray, double[] targetArray, double[] expectedCombined) {
        var combined = LinkFeatureCombiners.COSINE.combine(sourceArray, targetArray);
        assertThat(combined).containsExactly(expectedCombined, Offset.offset(1e-6));
    }

    static Stream<Arguments> l2InputArrays() {
        return Stream.of(
            Arguments.of(
                new double[]{5, 3.2, -4.2},                                 // sourceArray
                new double[]{-4.3, 7.2, 6.2},                               // targetArray
                new double[]{(5 + 4.3) * (5 + 4.3), 4 * 4, (10.4) * (10.4), 1.0} // expectation
            ),
            Arguments.of(
                new double[]{5.0, 0.0, -4.2},
                new double[]{5.0, 1.0, -4.2},
                new double[]{0.0, 1.0, 0.0, 1.0}
            )
        );
    }
    static Stream<Arguments> hadamardInputArrays() {
        return Stream.of(
            Arguments.of(
                new double[]{5, 3.2, -4.2},                                 // sourceArray
                new double[]{-4.3, 7.2, 6.2},                               // targetArray
                new double[]{-5 * 4.3, 3.2 * 7.2, -4.2 * 6.2, 1.0} // expectation
            ),
            Arguments.of(
                new double[]{5.0, 0.0, -4.2},
                new double[]{5.0, 1.0, -4.2},
                new double[]{25, 0.0, 4.2 * 4.2, 1.0}
            )
        );
    }
    static Stream<Arguments> cosineInputArrays() {
        var source1 = new double[]{5, 3.2, -4.2};
        var sL1 = L2Norm.l2Norm(source1);
        var target1 = new double[]{-4.3, 7.2, 6.2};
        var sT1 = L2Norm.l2Norm(target1);
        var source2 = new double[]{5.0, 0.0, -4.2};
        var target2 = new double[]{5.0, 1.0, -4.2};
        var sL2 = L2Norm.l2Norm(source2);
        var sT2 = L2Norm.l2Norm(target2);
        return Stream.of(
            Arguments.of(
                source1,                               // sourceArray
                target1,                               // targetArray
                new double[]{(-5 * 4.3 + 3.2 * 7.2 + -4.2 * 6.2) / (sL1 * sT1), 1.0} // expectation
            ),
            Arguments.of(
                source2,
                target2,
                new double[]{(25 + 0.0 + 4.2 * 4.2) / (sL2 * sT2), 1.0}
            )
        );
    }
}
