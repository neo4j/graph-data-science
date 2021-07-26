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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.LazyBatch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.L2FeatureStep;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkLogisticRegressionObjectiveTest {

    private static Stream<Arguments> featureBatches() {
        return Stream.of(
            Arguments.of(new LazyBatch(0, 2, 10), new Matrix(new double[]{0, 0, 1, 1}, 2, 2)),
            Arguments.of(new LazyBatch(4, 3, 10), new Matrix(new double[]{4, 4, 5, 5, 6, 6}, 3, 2))
        );
    }

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

    private LinkLogisticRegressionObjective objective;
    private final List<String> features = List.of("a", "b");

    @BeforeEach
    void setup() {
        var targets = HugeDoubleArray.newArray(graph.relationshipCount(), AllocationTracker.empty());
        targets.setAll(idx -> (idx < 2) ? 1 : 0);

        var linkFeatures = LinkFeatureExtractor.extractFeatures(
            graph,
            List.of(new L2FeatureStep(features))
        );
        this.objective = new LinkLogisticRegressionObjective(
            LinkLogisticRegressionData.from(features.size()),
            1.0,
            linkFeatures,
            targets
        );
    }

    @Test
    void makeTargets() {
        var batch = new LazyBatch(1, 2, graph.relationshipCount());
        var batchedTargets = objective.makeTargetsArray(batch);

        assertThat(batchedTargets.data()).isEqualTo(new Vector(1.0, 0.0));
    }

    @ParameterizedTest
    @MethodSource("featureBatches")
    void shouldComputeCorrectFeatures(Batch batch, Tensor<?> expected) {
        var featureCount = 2;

        var allFeatures = HugeObjectArray.newArray(double[].class, 10, AllocationTracker.empty());


        allFeatures.setAll(idx -> {
            double[] features = new double[featureCount];
            Arrays.fill(features, idx);
            return features;
        });

        Constant<Matrix> batchFeatures = objective.features(batch, allFeatures);

        assertThat(batchFeatures.data()).isEqualTo(expected);
    }

    @Test
    void loss() {
        var allRelsBatch = new LazyBatch(0, (int) graph.relationshipCount(), graph.relationshipCount());
        var loss = objective.loss(allRelsBatch, graph.relationshipCount());

        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        // weights are zero. penalty part of objective is 0. remaining part of logistic-loss is -Math.log(0.5).
        assertThat(lossValue).isEqualTo(-Math.log(0.5));
    }
}
