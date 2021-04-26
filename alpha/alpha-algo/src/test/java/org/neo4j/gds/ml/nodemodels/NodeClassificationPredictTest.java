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

import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;
import org.neo4j.logging.NullLog;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;

@GdlExtension
class NodeClassificationPredictTest {

    @GdlGraph
    private static final String TRAINED_GRAPH =
        "CREATE " +
        "  (n1:N {a: -1.36753705, b:  1.46853155})" +
        ", (n2:N {a: -1.45431768, b: -1.67820474})" +
        ", (n3:N {a: -0.34216825, b: -1.31498086})" +
        ", (n4:N {a: -0.60765016, b:  1.0186564})" +
        ", (n5:N {a: -0.48403364, b: -0.49152604})";

    @Inject
    private TestGraph graph;

    @Test
    void shouldPredict() {
        /*
         * Test validated using Python:
         *
         * import numpy as np
         * from scipy.special import softmax
         * nodeCount = 5
         * featureCount = 2
         * classCount = 4
         * features = np.hstack([np.random.randn(nodeCount, featureCount), np.ones((nodeCount, 1))])
         * features
         * weights = np.random.randn(classCount, featureCount + 1)
         * weights
         * result = softmax(np.matmul(features, weights.T), axis=1)
         * result
         */

        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(10);
        classIdMap.toMapped(1);
        classIdMap.toMapped(100);
        classIdMap.toMapped(2);
        List<String> featureProperties = List.of("a", "b");
        var modelData = NodeLogisticRegressionData.builder()
            .weights(new Weights<>(new Matrix(new double[]{
                1.12730619, -0.84532386, 0.93216654,
                1.63908065, -0.08391665, -1.46620738,
                -1.07448415, 1.19160801, 0.70054154,
                -0.63303538, 0.08735695, -3.39978931
            }, 4, 3)))
            .classIdMap(classIdMap)
            .build();

        var result = new NodeClassificationPredict(
            new NodeLogisticRegressionPredictor(modelData, featureProperties),
            graph,
            1,
            1,
            true,
            featureProperties,
            AllocationTracker.empty(),
            TestProgressLogger.NULL_LOGGER
        ).compute();

        assertThat(result.predictedProbabilities())
            .isPresent()
            .get()
            .satisfies(probabilities -> {
                assertThat(probabilities.get(graph.toMappedNodeId("n1"))).contains(
                    new double[]{3.10101084e-03, 4.28113983e-04, 9.94690586e-01, 1.78028917e-03},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n2"))).contains(
                    new double[]{5.92933237e-01, 7.13225933e-03, 3.78861506e-01, 2.10729975e-02},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n3"))).contains(
                    new double[]{8.68989387e-01, 2.43518571e-02, 1.00540662e-01, 6.11809424e-03},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n4"))).contains(
                    new double[]{3.94945880e-02, 5.71116040e-03, 9.50882595e-01, 3.91165643e-03},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n5"))).contains(
                    new double[]{5.22359811e-01, 2.54830927e-02, 4.41981132e-01, 1.01759637e-02},
                    Offset.offset(1e-6)
                );
            });

        assertEquals(100, result.predictedClasses().get(graph.toMappedNodeId("n1")));
        assertEquals(10, result.predictedClasses().get(graph.toMappedNodeId("n2")));
        assertEquals(10, result.predictedClasses().get(graph.toMappedNodeId("n3")));
        assertEquals(100, result.predictedClasses().get(graph.toMappedNodeId("n4")));
        assertEquals(10, result.predictedClasses().get(graph.toMappedNodeId("n5")));
    }

    @Test
    void singleClass() {
        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(1);
        List<String> featureProperties = List.of("a", "b");
        var modelData = NodeLogisticRegressionData.builder()
            .weights(new Weights<>(new Matrix(new double[]{
                1.12730619, -0.84532386, 0.93216654
            }, 1, 3)))
            .classIdMap(classIdMap)
            .build();

        var result = new NodeClassificationPredict(
            new NodeLogisticRegressionPredictor(modelData, featureProperties),
            graph,
            1,
            1,
            true,
            featureProperties,
            AllocationTracker.empty(),
            TestProgressLogger.NULL_LOGGER
        ).compute();

        assertThat(result.predictedProbabilities())
            .isPresent()
            .get()
            .satisfies(probabilities -> {
                assertThat(probabilities.get(graph.toMappedNodeId("n1"))).contains(
                    new double[]{1},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n2"))).contains(
                    new double[]{1},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n3"))).contains(
                    new double[]{1},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n4"))).contains(
                    new double[]{1},
                    Offset.offset(1e-6)
                );
                assertThat(probabilities.get(graph.toMappedNodeId("n5"))).contains(
                    new double[]{1},
                    Offset.offset(1e-6)
                );
            });

        assertEquals(1, result.predictedClasses().get(graph.toMappedNodeId("n1")));
        assertEquals(1, result.predictedClasses().get(graph.toMappedNodeId("n2")));
        assertEquals(1, result.predictedClasses().get(graph.toMappedNodeId("n3")));
        assertEquals(1, result.predictedClasses().get(graph.toMappedNodeId("n4")));
        assertEquals(1, result.predictedClasses().get(graph.toMappedNodeId("n5")));
    }

    @Test
    void shouldLogProgress() {
        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(0);
        var featureProperties = List.of("a", "b");
        var modelName = "model";
        var model = Model.of(
            "",
            modelName,
            "",
            GraphSchema.empty(),
            NodeLogisticRegressionData.builder()
                .weights(new Weights<>(new Matrix(new double[]{
                    1.12730619, -0.84532386, 0.93216654
                }, 1, 3)))
                .classIdMap(classIdMap)
                .build(),
            ImmutableNodeClassificationTrainConfig
                .builder()
                .modelName(modelName)
                .targetProperty("foo")
                .featureProperties(featureProperties)
                .holdoutFraction(0.2)
                .validationFolds(4)
                .addParam(Map.of("penalty", 1.0))
                .build()
        );
        ModelCatalog.set(model);

        var mcnlrPredict = new NodeClassificationPredictAlgorithmFactory<>(TestProgressLogger.FACTORY).build(
            graph,
            ImmutableNodeClassificationMutateConfig.builder()
                .mutateProperty("foo")
                .modelName(modelName)
                .concurrency(2)
                .batchSize(1)
                .build(),
            AllocationTracker.empty(),
            NullLog.getInstance(),
            EmptyProgressEventTracker.INSTANCE
        );
        mcnlrPredict.compute();

        var messagesInOrder = ((TestProgressLogger) mcnlrPredict.getProgressLogger()).getMessages(INFO);

        AssertionsForInterfaceTypes.assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .doesNotHaveDuplicates()
            .hasSize(7)
            .containsExactly(
                "NodeLogisticRegressionPredict :: Start",
                "NodeLogisticRegressionPredict 20%",
                "NodeLogisticRegressionPredict 40%",
                "NodeLogisticRegressionPredict 60%",
                "NodeLogisticRegressionPredict 80%",
                "NodeLogisticRegressionPredict 100%",
                "NodeLogisticRegressionPredict :: Finished"
            );
        ModelCatalog.drop("", modelName);
    }
}
