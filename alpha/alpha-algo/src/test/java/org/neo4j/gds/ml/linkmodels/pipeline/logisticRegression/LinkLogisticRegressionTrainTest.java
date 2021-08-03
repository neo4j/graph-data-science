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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.L2FeatureStep;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkLogisticRegressionTrainTest {

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

    List<String> features = List.of("a", "b");
    private HugeObjectArray<double[]> linkFeatures;
    private HugeDoubleArray targets;
    private HugeLongArray trainSet;

    @BeforeEach
    void setup() {
        this.targets = HugeDoubleArray.newArray(graph.relationshipCount(), AllocationTracker.empty());
        targets.setAll(idx -> (idx < 2) ? 1 : 0);

        this.trainSet = HugeLongArray.newArray(graph.relationshipCount(), AllocationTracker.empty());
        trainSet.setAll(i -> i);

        this.linkFeatures = LinkFeatureExtractor.extractFeatures(
            graph,
            List.of(new L2FeatureStep(features)),
            4
        );
    }

    @Test
    void shouldComputeWithStreakStopper() {
        var config = LinkLogisticRegressionTrainConfig.of(1, Map.of("maxEpochs", 100000, "tolerance", 1e-4));

        var linearRegression = new LinkLogisticRegressionTrain(trainSet, linkFeatures, targets, config, ProgressTracker.NULL_TRACKER);

        var result = linearRegression.compute();

        var expected = new Matrix(new double[]{1.4312096002404944, -1.0723221599949817}, 1, 2);
        assertThat(result.weights().data()).matches(matrix -> matrix.equals(expected, 1e-8));
    }

    @Test
    void shouldComputeWithStreakStopperConcurrently() {
        var config = LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1.0, "maxEpochs", 100, "tolerance", 1e-10, "batchSize", 1));
        var linearRegression = new LinkLogisticRegressionTrain(trainSet, linkFeatures, targets, config, ProgressTracker.NULL_TRACKER);

        var result = linearRegression.compute();

        var expected = new Matrix(new double[]{0.09696345464435131, -0.09306922756442035}, 1, 2);
        assertThat(result.weights().data()).matches(matrix -> matrix.equals(expected, 1e-8));
    }

    @Test
    void usingPenaltyShouldGiveSmallerAbsoluteValueWeights() {
        var config = LinkLogisticRegressionTrainConfig.of(1, Map.of("maxEpochs", 100000, "tolerance", 1e-4));
        var configWithPenalty = LinkLogisticRegressionTrainConfig.of(1, Map.of("maxEpochs", 100000, "tolerance", 1e-4,  "penalty", 1));

        Matrix result = new LinkLogisticRegressionTrain(trainSet,  linkFeatures, targets, config, ProgressTracker.NULL_TRACKER)
            .compute()
            .weights()
            .data();

        Matrix resultWithPenality = new LinkLogisticRegressionTrain(trainSet,  linkFeatures, targets, configWithPenalty, ProgressTracker.NULL_TRACKER)
            .compute()
            .weights()
            .data();

        assertThat(result.dimensions()).isEqualTo(resultWithPenality.dimensions());

        double penalizedSumOfSquares = resultWithPenality.map(v -> v * v).aggregateSum();
        double sumOfSquares = result.map(v -> v * v).aggregateSum();

        assertThat(penalizedSumOfSquares).isLessThan(sumOfSquares);

    }
}
