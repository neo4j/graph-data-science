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
package org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LinkLogisticRegressionPredictorTest {

    private static final double[] WEIGHTS = new double[]{.5, .6, .7, .8};

    private static Stream<Arguments> inputs() {
        return Stream.of(
            Arguments.of(
                new double[] {0.21, -0.07, -0.14, 0.21}, 1 / (1 + Math.pow(Math.E, -1 * (0.21 * .5 + (-0.07) * .6 + (-0.14) * .7 + 0.21 * .8)))
            ),
            Arguments.of(
                new double[] {0.14, -0.08, 0.08, 0.24}, 1 / (1 + Math.pow(Math.E, -1 * (0.14 * .5 + (-0.08) * .6 +  0.08 * .7 + 0.24 * .8)))
            ),
            Arguments.of(
                new double[]{0.28, 0.56, -0.28, 0.56}, 1 / (1 + Math.pow(Math.E, -1 * (0.28 * .5 + 0.56 * .6 +  (-0.28) * .7 + 0.56 * .8)))
            )
        );
    }

    @MethodSource("inputs")
    @ParameterizedTest
    void computesProbability(double[] features, double expectedResult) {
        var modelData = ImmutableLinkLogisticRegressionData.of(
            new Weights<>(new Matrix(WEIGHTS, 1, WEIGHTS.length)),
            Weights.ofScalar(0)
        );

        var predictor = new LinkLogisticRegressionPredictor(modelData);

        var result = predictor.predictedProbability(features);

        assertThat(result).isCloseTo(expectedResult, Offset.offset(1e-8));
    }
}
