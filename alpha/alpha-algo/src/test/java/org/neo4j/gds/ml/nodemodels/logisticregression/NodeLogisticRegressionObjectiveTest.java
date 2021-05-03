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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.ml.batch.LazyBatch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodeLogisticRegressionObjectiveTest {

    private static final int NUMBER_OF_CLASSES = 3;

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:N {a: 2.0, b: 1.2, t: 1})" +
        ", (n2:N {a: 1.3, b: 0.5, t: 0})" +
        ", (n3:N {a: 0.0, b: 2.8, t: 2})" +
        ", (n4:N {a: 1.0, b: 0.9, t: 1})";

    @Inject
    private Graph graph;

    @Test
    void shouldProduceCorrectLoss() {
        var allNodesBatch = new LazyBatch(0, (int) graph.nodeCount(), graph.nodeCount());
        var featureProps = List.of("a", "b");
        var predictor = new NodeLogisticRegressionPredictor(NodeLogisticRegressionData.from(graph, featureProps, "t"), featureProps);
        var objective = new NodeLogisticRegressionObjective(graph, predictor, "t", 0.0);
        var loss = objective.loss(allNodesBatch, 4);
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        // weights are zero => each class has equal probability to be correct.
        assertThat(lossValue).isCloseTo(-log(1.0 / NUMBER_OF_CLASSES), Offset.offset(1e-10));
    }

    @Test
    void shouldSortClasses() {
        var featureProps = List.of("a", "b");
        var predictor = new NodeLogisticRegressionPredictor(NodeLogisticRegressionData.from(graph, featureProps, "t"), featureProps);
        var objective = new NodeLogisticRegressionObjective(graph, predictor, "t", 0.0);
        var classList = objective.modelData().classIdMap().originalIdsList();
        assertThat(classList).containsExactly(0L, 1L, 2L);
    }

    @Test
    void shouldEstimateMemoryUsage() {
        var memoryUsageInBytes = NodeLogisticRegressionObjective.sizeOfBatchInBytes(100, 10, 10);

        var weightGradient = 8 * 10 * 10 + 16;      // 8 bytes for a double * numberOfClasses * numberOfFeatures + 16 for the double array
        var makeTargets = 8 * 100 * 1 + 16;         // 8 bytes for a double * batchSize * 1 for the single target property + 16 for the double array
        var weightedFeatures = 8 * 100 * 10 + 16;   // 8 bytes for a double * batchSize * numberOfClasses + 16 for the double array
        var softMax = 8 * 100 * 10 + 16;            // 8 bytes for a double * batchSize * numberOfClasses + 16 for the double array
        var unpenalizedLoss = 24;                   // 8 bytes for a double + 16 for the double array
        var l2norm = 8 + 16;                        // 8 bytes for a double + 16 for the double array
        var constantScale = 8 + 16;                 // 8 bytes for a double + 16 for the double array
        var elementSum = 8 + 16;                    // 8 bytes for a double + 16 for the double array
        var predictor = 24304;                      // black box, not from this class

        var trainEpoch = makeTargets +
                         weightedFeatures +
                         softMax +
                         2 * unpenalizedLoss +
                         2 * l2norm +
                         2 * constantScale +
                         2 * elementSum +
                         predictor;

        var evaluateLoss = makeTargets +
                           weightedFeatures +
                           softMax +
                           unpenalizedLoss +
                           l2norm +
                           constantScale +
                           elementSum +
                           predictor;

        var expected = max(trainEpoch + weightGradient, evaluateLoss);
        assertThat(makeTargets).isEqualTo(NodeLogisticRegressionObjective.costOfMakeTargets(100));
        assertThat(memoryUsageInBytes).isEqualTo(expected);
    }

}
