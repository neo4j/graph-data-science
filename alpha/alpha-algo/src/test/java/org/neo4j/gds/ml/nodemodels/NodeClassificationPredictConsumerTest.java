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
package org.neo4j.gds.ml.nodemodels;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.BatchTransformer;
import org.neo4j.gds.ml.core.batch.ListBatch;
import org.neo4j.gds.ml.core.batch.SingletonBatch;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@GdlExtension
class NodeClassificationPredictConsumerTest {


    @GdlGraph
    private static final String GDL =
        "({class: 0, nanembedding: [NaN, NaN]}), ({class: 1, nanembedding: [NaN, NaN]})";

    @Inject
    Graph graph;

    static LocalIdMap idMapOf(long... ids) {
        LocalIdMap idMap = new LocalIdMap();
        for (long id : ids) {
            idMap.toMapped(id);
        }

        return idMap;
    }

    @Test
    void canProducePredictions() {
        Predictor<Matrix, NodeLogisticRegressionData> predictor = new Predictor<>() {
            @Override
            public NodeLogisticRegressionData modelData() {
                return NodeLogisticRegressionData
                    .builder()
                    .classIdMap(idMapOf(0, 1))
                    // weights are required but not used
                    .weights(Weights.ofMatrix(1, 1))
                    .build();
            }

            @Override
            public Matrix predict(Graph graph, Batch batch) {
                double[] data = new double[2];
                for (long id : batch.nodeIds()) {
                    if (id == 0) {
                        data = new double[]{0.2, 0.8};
                    } else {
                        data = new double[]{0.7, 0.3};
                    }
                }
                return new Matrix(data, 1, 2);
            }
        };
        var probabilities = HugeObjectArray.newArray(double[].class, 2);
        var predictedClasses = HugeLongArray.newArray(2);
        var predictConsumer = new NodeClassificationPredictConsumer(
            graph,
            BatchTransformer.IDENTITY,
            predictor,
            probabilities,
            predictedClasses,
            List.of("class"),
            ProgressTracker.NULL_TRACKER
        );

        predictConsumer.accept(new SingletonBatch(0));
        predictConsumer.accept(new SingletonBatch(1));

        assertThat(probabilities.get(0)).containsExactly(0.2, 0.8);
        assertThat(probabilities.get(1)).containsExactly(0.7, 0.3);
        assertThat(predictedClasses.get(0)).isEqualTo(1);
        assertThat(predictedClasses.get(1)).isEqualTo(0);
    }

    @Test
    void shouldThrowWhenFeaturePropertiesContainNaN() {
        var featureProperties = List.of("nanembedding");
        var data = NodeLogisticRegressionData.from(graph, featureProperties, "class");
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            BatchTransformer.IDENTITY,
            new NodeLogisticRegressionPredictor(data, featureProperties),
            null,
            HugeLongArray.of(0, 1),
            featureProperties,
            ProgressTracker.NULL_TRACKER
        );

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> consumer.accept(new ListBatch(List.of(0L, 1L))))
            .withMessage("Node with ID `0` has invalid feature property value NaN for property `nanembedding`");
    }

}
