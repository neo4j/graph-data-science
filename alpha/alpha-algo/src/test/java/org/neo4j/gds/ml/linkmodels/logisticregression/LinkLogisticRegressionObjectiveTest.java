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

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.LazyBatch;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.core.functions.Sigmoid.sigmoid;

@GdlExtension
class LinkLogisticRegressionObjectiveTest {

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (n1:N {a: 2.0, b: 1.2})" +
        ", (n2:N {a: 1.3, b: 0.5})" +
        ", (n3:N {a: 0.0, b: 2.8})" +
        ", (n4:N {a: 1.0, b: 0.9})" +
        ", (n5:N {a: 1.0, b: 0.9})" +
        ", (n1)-[:T {label: 1.0}]->(n2)" +
        ", (n3)-[:T {label: 1.0}]->(n4)" +
        ", (n1)-[:T {label: 0.0}]->(n3)" +
        ", (n2)-[:T {label: 0.0}]->(n4)";

    @Inject
    private Graph graph;

    private Batch allNodesBatch;

    @BeforeEach
    void setup() {
        allNodesBatch = new LazyBatch(0, (int) graph.nodeCount(), graph.nodeCount());
    }

    @Test
    void shouldProduceCorrectLoss() {
        List<String> featureProperties = List.of("a", "b");
        var extractors = FeatureExtraction.propertyExtractors(graph, featureProperties);
        var data = LinkLogisticRegressionData.from(
            graph,
            featureProperties,
            LinkFeatureCombiners.L2
        );
        var objective = new LinkLogisticRegressionObjective(data, featureProperties, extractors, 1.0, graph);
        var loss = objective.loss(allNodesBatch, graph.relationshipCount());
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();
        // zero penalty since weights are zero
        Assertions.assertThat(lossValue).isEqualTo(-Math.log(0.5));
    }

    @Test
    void shouldProduceCorrectLossWithNonZeroWeights() {
        List<String> featureProperties = List.of("a", "b");
        var extractors = FeatureExtraction.propertyExtractors(graph, featureProperties);
        var data = LinkLogisticRegressionData.from(
            graph,
            featureProperties,
            LinkFeatureCombiners.L2
        );
        var objective = new LinkLogisticRegressionObjective(data, featureProperties, extractors, 1.0, graph);
        // we proceed to "injecting" non-zero values for the weights of the model
        var weights = objective.weights();
        var backingArray = weights.get(0).data().data();
        backingArray[0] = 0.2;
        backingArray[1] = -0.3;
        backingArray[2] = 0.5;
        var loss = objective.loss(allNodesBatch, graph.relationshipCount());
        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        var pred = new double[]{
            sigmoid(0.2 * 0.49 - 0.3 * 0.49 + 0.5),
            sigmoid(0.2 * 4 - 0.3 * 2.56 + 0.5),
            sigmoid(0.2 * 0.09 - 0.3 * 0.16 + 0.5),
            sigmoid(0.2 * 1.0 - 0.3 * 3.61 + 0.5)
        };
        var expectedTotalCEL = -Math.log(pred[0]) - Math.log(1 - pred[1]) - Math.log(1 - pred[2]) - Math.log(pred[3]);
        var expectedPenalty = Math.pow(0.2, 2) + Math.pow(0.3, 2);
        Assertions.assertThat(lossValue).isCloseTo(expectedPenalty + expectedTotalCEL/4, Offset.offset(1e-7));
    }

    @Test
    void shouldEstimateMemoryUsage() {
        var memoryUsageInBytes = LinkLogisticRegressionObjective.sizeOfBatchInBytes(100, 10);

        var weightGradient = 8 * 10 + 16;           // 8 bytes for a double * numberOfFeatures + 16 for the double array
        var makeTargets = 8 * 100 + 16;             // 8 bytes for a double * batchSize + 16 for the double array
        var weightedFeatures = 8 * 100 + 16;        // 8 bytes for a double * batchSize + 16 for the double array
        var softMax = 8 * 100 + 16;                 // 8 bytes for a double * batchSize + 16 for the double array
        var unpenalizedLoss = 24;                   // 8 bytes for a double + 16 for the double array
        var l2norm = 24;                            // 8 bytes for a double + 16 for the double array
        var constantScale = 24;                     // 8 bytes for a double + 16 for the double array
        var elementSum = 24;                        // 8 bytes for a double + 16 for the double array
        var predictor = 9968;                       // from LinkLogisticRegressionPredictorTest

        var trainEpoch = makeTargets +
                         weightedFeatures +
                         softMax +
                         2 * unpenalizedLoss +
                         2 * l2norm +
                         2 * constantScale +
                         2 * elementSum +
                         predictor;

        var expected = trainEpoch + weightGradient;
        assertThat(makeTargets).isEqualTo(LinkLogisticRegressionObjective.costOfMakeTargets(100));
        assertThat(memoryUsageInBytes).isEqualTo(expected);
    }

}
