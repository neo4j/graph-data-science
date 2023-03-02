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
package org.neo4j.gds.ml.models.mlp;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.batch.RangeBatch;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MLPClassifierTest {

    private List<Weights<Matrix>> weights;

    private List<Weights<Vector>> biases;

    @BeforeEach
    void setup() {
        weights = List.of(new Weights<>(new Matrix(new double[]{
            1,-1,1,-1,
            1,-1,1,-1,
            1,-1,1,-1,
            1,-1,1,-1
        }, 4, 4)),
            new Weights<>(new Matrix(new double[]{
            2,2,2,-2,
            1,1,1,-1
        }, 2, 4))
        );
        biases = List.of(Weights.ofVector(1,1,1,1), Weights.ofVector(-1,-1));
    }

    private static Stream<Arguments> inputs() {
        return Stream.of(
            Arguments.of(
                new double[] {1, -2, -1, 2},
                new double[]{0.880797077, 0.119202922}
            )
        );
    }

    @MethodSource("inputs")
    @ParameterizedTest
    void shouldPredictProbabilities(double[] features, double[] expectedResult) {
        var modelData = ImmutableMLPClassifierData.of(weights, biases);
        var predictor = new MLPClassifier(modelData);
        var result = predictor.predictProbabilities(features);
        assertThat(result).containsExactly(expectedResult, Offset.offset(1e-8));
    }

    @Test
    void batchingGivesEquivalentResults() {
        var featureCount = 4;
        var modelData = ImmutableMLPClassifierData.of(weights, biases);

        var classifier = new MLPClassifier(modelData);
        var random = new Random();
        var featureData = HugeObjectArray.newArray(double[].class, 10);
        for (int i = 0; i < 10; i++) {
            featureData.set(i, random.doubles(featureCount).toArray());
        }
        var features = FeaturesFactory.wrap(featureData);

        var probabilityMatrix = classifier.predictProbabilities(new RangeBatch(0, 10, 10), features);
        for (int i = 0; i < 10; i++) {
            var singlePrediction = classifier.predictProbabilities(features.get(i));
            var batchPrediction = probabilityMatrix.getRow(i);
            assertThat(singlePrediction).containsExactly(batchPrediction);
        }
    }

}
