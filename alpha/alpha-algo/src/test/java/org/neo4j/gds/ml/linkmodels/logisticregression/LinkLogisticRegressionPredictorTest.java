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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.LinkFeatureCombiner;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkLogisticRegressionPredictorTest {

    @GdlGraph
    static String GRAPH =
        "(a {x: .33, y: [-1.2, 2.1, 3.0]}), " +
        "(b {x: .66, y: [7.5, 77.1, .077]}), " +
        "(c {x: .99, y: [66.6, 33.3, 11.1]}), " +
        "(d {x: 2.42, y: [.808, .08, 80.808]}), ";

    @Inject
    Graph graph;

    @Inject
    IdFunction idFunction;

    private static final double[] WEIGHTS = new double[]{.5, .6, .7, .8, .9};

    private static Stream<Arguments> inputs() {
        return Stream.of(
            Arguments.of(
                "a", "b", 1 / (1 + Math.pow(Math.E, -1 * (.99 * .5 + 6.3 * .6 + 79.2 * .7 + 3.077 * .8 + WEIGHTS[4])))
            ),
            Arguments.of(
                "a", "c", 1 / (1 + Math.pow(Math.E, -1 * (1.32 * .5 + 65.3 * .6 + 35.4 * .7 + 14.1 * .8 + WEIGHTS[4])))
            ),
            Arguments.of(
                "c",
                "b",
                1 / (1 + Math.pow(Math.E, -1 * (1.65 * .5 + 74.1 * .6 + 110.4 * .7 + 11.177 * .8 + WEIGHTS[4])))
            ),
            Arguments.of(
                "d",
                "a",
                1 / (1 + Math.pow(Math.E, -1 * (2.75 * .5 + -.392 * .6 + 2.18 * .7 + 83.808 * .8 + WEIGHTS[4])))
            )
        );
    }

    @MethodSource("inputs")
    @ParameterizedTest
    void computesProbability(String sourceNode, String targetNode, double expectedResult) {
        List<String> featureProperties = List.of("x", "y");
        var extractors = FeatureExtraction.propertyExtractors(graph, featureProperties);
        var modelData = LinkLogisticRegressionData
            .builder()
            .from(LinkLogisticRegressionData.from(graph, featureProperties, new SumCombiner()))
            .weights(new Weights<>(new Matrix(WEIGHTS, 1, WEIGHTS.length)))
            .build();
        var predictor = new LinkLogisticRegressionPredictor(modelData, featureProperties, extractors);

        var result = predictor.predictedProbability(idFunction.of(sourceNode), idFunction.of(targetNode));

        assertThat(result).isCloseTo(expectedResult, Offset.offset(1e-10));
    }

    @Test
    public void shouldEstimateMemoryUsage() {
        var memoryUsageInBytes = LinkLogisticRegressionPredictor.sizeOfBatchInBytes(100, 10);

        int memoryUsageOfFeatureExtractors = 320; // 32 bytes * number of features
        int memoryUsageOfFeatureMatrix = 8016; // 8 bytes * batch size * number of features + 16
        int memoryUsageOfMatrixMultiplication = 816; // 8 bytes per double * batchSize + 16
        int memoryUsageOfSigmoid = 816; // 8 bytes per double * batchSize + 16
        // total 9888
        assertThat(memoryUsageInBytes).isEqualTo(memoryUsageOfFeatureExtractors + memoryUsageOfFeatureMatrix + memoryUsageOfMatrixMultiplication + memoryUsageOfSigmoid);
    }

    private static class SumCombiner implements LinkFeatureCombiner {
        @Override
        public double[] combine(double[] sourceArray, double[] targetArray) {
            var result =  new double[sourceArray.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = sourceArray[i] + targetArray[i];
            }
            return result;
        }

        @Override
        public int linkFeatureDimension(int nodeFeatureDimension) {
            return nodeFeatureDimension;
        }
    }
}
