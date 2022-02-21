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
package org.neo4j.gds.ml.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.Trainer;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.LazyBatch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainer.LogisticRegressionData;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LogisticRegressionObjectiveTest {

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

    private LogisticRegressionObjective objective;
    private final List<String> features = List.of("a", "b");

    @BeforeEach
    void setup() {
        var labels = HugeLongArray.newArray(graph.relationshipCount());
        labels.setAll(idx -> (idx < 2) ? 1 : 0);

        // L2 Norm features for node properties a,b
        var linkFeatures = HugeObjectArray.of(
            new double[]{Math.pow(0.7, 2), Math.pow(0.7, 2)},
            new double[]{Math.pow(-1, 2), Math.pow(1.7, 2)},
            new double[]{Math.pow(1, 2), Math.pow(-1.6, 2)},
            new double[]{Math.pow(0.3, 2), Math.pow(-0.4, 2)}
        );

        var classifier = new LogisticRegressionClassifier(LogisticRegressionData.of(features.size(), labels, true));
        this.objective = new LogisticRegressionObjective(
            classifier,
            1.0,
            Trainer.Features.wrap(linkFeatures),
            labels
        );
    }

    @Test
    void makeTargets() {
        var batch = new LazyBatch(1, 2, graph.relationshipCount());
        var batchedTargets = objective.batchLabelVector(batch, objective.modelData().classIdMap());

        // original labels are 1.0, 0.0 , but these are local ids. since nodeId 0 has label 1.0, this maps to 0.0.
        assertThat(batchedTargets.data()).isEqualTo(new Vector(0.0, 1.0));
    }

    @ParameterizedTest
    @MethodSource("featureBatches")
    void shouldComputeCorrectFeatures(Batch batch, Tensor<?> expected) {
        var featureCount = 2;

        var allFeatures = HugeObjectArray.newArray(double[].class, 10);


        allFeatures.setAll(idx -> {
            double[] features = new double[featureCount];
            Arrays.fill(features, idx);
            return features;
        });

        Constant<Matrix> batchFeatures = LogisticRegressionClassifier.batchFeatureMatrix(batch,
            Trainer.Features.wrap(allFeatures));

        assertThat(batchFeatures.data()).isEqualTo(expected);
    }

    @Test
    void loss() {
        var allRelsBatch = new LazyBatch(0, (int) graph.relationshipCount(), graph.relationshipCount());
        var loss = objective.loss(allRelsBatch, graph.relationshipCount());

        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        // weights are zero. penalty part of objective is 0. remaining part of CEL is -Math.log(0.5).
        assertThat(lossValue).isEqualTo(-Math.log(0.5), Offset.offset(1E-9));
    }

    @ParameterizedTest
    @CsvSource(value = {
        " 10,   1, false,  808",
        " 10,   1, true, 1_000",
        "100,   1, true, 6_760",
        " 10, 100, true, 9_712",
    })
    void shouldEstimateMemoryUsage(int batchSize, int featureDim, boolean useBias, long expected) {
        var memoryUsageInBytes = LogisticRegressionObjective.sizeOfBatchInBytes(batchSize, featureDim, useBias);
        assertThat(memoryUsageInBytes).isEqualTo(expected);
    }
}
