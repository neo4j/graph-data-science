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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.DoubleArrayCombiner;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

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

    @Test
    void computesProbability() {
        var weights = new double[]{.5, .6, .7, .8, .9};
        var modelData = LinkLogisticRegressionData
            .builder()
            .from(LinkLogisticRegressionData.from(graph, List.of("x", "y"), new SumCombiner()))
            .weights(new Weights<>(new Matrix(weights, 1, 5)))
            .build();
        var predictor = new LinkLogisticRegressionPredictor(modelData);

        var result = predictor.predictedProbability(graph, idFunction.of("a"), idFunction.of("b"));

        var expectedResult = 1 / (1 + Math.pow(Math.E, -1 * (.99 * .5 + 6.3 * .6 + 79.2 * .7 + 3.077 * .8 + .9)));

        assertThat(result).isCloseTo(expectedResult, Offset.offset(1e-10));
    }

    private static class SumCombiner implements DoubleArrayCombiner {
        @Override
        public double[] combine(double[] sourceArray, double[] targetArray) {
            var result =  new double[sourceArray.length];
            for (int i = 0; i < result.length; i++) {
                result[i] = sourceArray[i] + targetArray[i];
            }
            return result;
        }
    }
}
