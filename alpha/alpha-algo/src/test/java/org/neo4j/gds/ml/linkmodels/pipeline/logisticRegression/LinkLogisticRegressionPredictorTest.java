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
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
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
        "(a {x: 0.33, y: [-0.1, 0.2, 0.3]}), " +
        "(b {x: 0.66, y: [0.7, -0.7, 0.7]}), " +
        "(c {x: 0.42, y: [0.8, 0.4, 0.8]}), ";

    @Inject
    Graph graph;

    @Inject
    IdFunction idFunction;

    private static final double[] WEIGHTS = new double[]{.5, .6, .7, .8};

    private static Stream<Arguments> inputs() {
        return Stream.of(
            Arguments.of(
                "a", "b", 1 / (1 + Math.pow(Math.E, -1 * ((.33 * .66) * .5 + (-0.1 * 0.7) * .6 + (0.2 * -0.7) * .7 + (0.3 * 0.7) * .8)))
            ),
            Arguments.of(
                "a", "c", 1 / (1 + Math.pow(Math.E, -1 * ((.33 * .42) * .5 + (-0.1 * 0.8) * .6 + (0.2 * 0.4) * .7 + (0.3 * 0.8) * .8)))
            ),
            Arguments.of(
                "b", "c", 1 / (1 + Math.pow(Math.E, -1 * ((.66 * .42) * .5 + (0.7 * 0.8) * .6 + (-0.7 * 0.4) * .7 + (0.7 * 0.8) * .8)))
            )
        );
    }

    @MethodSource("inputs")
    @ParameterizedTest
    void computesProbability(String sourceNode, String targetNode, double expectedResult) {
        List<String> featureProperties = List.of("x", "y");
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, List.of(new HadamardFeatureStep(featureProperties)));
        var modelData = ImmutableLinkLogisticRegressionData.of(new Weights<>(new Matrix(WEIGHTS, 1, WEIGHTS.length)));

        var predictor = new LinkLogisticRegressionPredictor(modelData, linkFeatureExtractor);

        var result = predictor.predictedProbability(idFunction.of(sourceNode), idFunction.of(targetNode));

        assertThat(result).isCloseTo(expectedResult, Offset.offset(1e-8));
    }
}
