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
package org.neo4j.gds.ml.nodeClassification;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.LogAdapter;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

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
        List<String> featureProperties = List.of("a", "b");
        var modelData = ImmutableLogisticRegressionData.builder()
            .weights(new Weights<>(new Matrix(new double[]{
                1.12730619, -0.84532386,
                1.63908065, -0.08391665,
                -1.07448415, 1.19160801,
                -0.63303538, 0.08735695
            }, 4, 2)))
            .bias(Weights.ofVector(
                0.93216654,
                -1.46620738,
                0.70054154,
                -3.39978931
            ))
            .numberOfClasses(4)
            .build();

        var result = new NodeClassificationPredict(
            LogisticRegressionClassifier.from(modelData),
            FeaturesFactory.extractLazyFeatures(graph, featureProperties),
            1,
            new Concurrency(1),
            true,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
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

        assertEquals(2, result.predictedClasses().get(graph.toMappedNodeId("n1")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n2")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n3")));
        assertEquals(2, result.predictedClasses().get(graph.toMappedNodeId("n4")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n5")));
    }

    @Test
    void singleClass() {
        List<String> featureProperties = List.of("a", "b");
        var modelData = ImmutableLogisticRegressionData.builder()
            .weights(new Weights<>(new Matrix(new double[]{
                1.12730619, -0.84532386
            }, 1, 2)))
            .bias(Weights.ofVector(0.93216654))
            .numberOfClasses(1)
            .build();

        var result = new NodeClassificationPredict(
            LogisticRegressionClassifier.from(modelData),
            FeaturesFactory.extractLazyFeatures(graph, featureProperties),
            1,
            new Concurrency(1),
            true,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
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

        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n1")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n2")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n3")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n4")));
        assertEquals(0, result.predictedClasses().get(graph.toMappedNodeId("n5")));
    }

    @Test
    void shouldLogProgress() {
        var featureProperties = List.of("a", "b");
        LogisticRegressionData modelData = ImmutableLogisticRegressionData.builder()
            .weights(new Weights<>(new Matrix(new double[]{
                1.12730619, -0.84532386
            }, 1, 2)))
            .bias(Weights.ofVector(
                0.93216654
            ))
            .numberOfClasses(1)
            .build();

        var log = Neo4jProxy.testLog();
        var concurrency = new Concurrency(1);
        var progressTracker = new TaskProgressTracker(
            NodeClassificationPredict.progressTask(graph.nodeCount()),
            new LogAdapter(log),
            concurrency,
            new JobId(),
            EmptyTaskRegistryFactory.INSTANCE,
            EmptyUserLogRegistryFactory.INSTANCE
        );

        var mcnlrPredict = new NodeClassificationPredict(
            ClassifierFactory.create(modelData),
            FeaturesFactory.extractLazyFeatures(graph, featureProperties),
            100,
            concurrency,
            false,
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );

        mcnlrPredict.compute();

        var messagesInOrder = log.getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .doesNotHaveDuplicates()
            .hasSize(3)
            .containsExactly(
                "Node classification predict :: Start",
                "Node classification predict 100%",
                "Node classification predict :: Finished"
            );
    }

    @Test
    void shouldEstimateMemory() {
        var batchSize = 10;
        var featureCount = 50;
        var classCount = 10;
        var produceProbabilities = false;
        var nodeCount = 1000;
        var concurrency = new Concurrency(1);

        // one thousand longs, plus overhead of a HugeLongArray
        var predictedClasses = 8 * 1000 + 40;
        // The predictions variable is tested elsewhere
        var predictionsVariable = LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(batchSize, featureCount, classCount, classCount);

        var expected = predictedClasses + predictionsVariable;

        var estimate = NodeClassificationPredict.memoryEstimation(produceProbabilities, batchSize, featureCount, classCount)
            .estimate(GraphDimensions.of(nodeCount), concurrency);
        assertMemoryRange(estimate.memoryUsage(), expected);
    }

    @Test
    void memoryEstimationShouldScaleWithNodeCountWhenProbabilitiesAreRecorded() {
        var batchSize = 1000;
        var featureCount = 500;
        var classCount = 100;
        var produceProbabilities = true;
        var concurrency = new Concurrency(1);

        var smallGraphNodeCount = 1_000_000;
        var smallToLargeFactor = 1000;

        var estimation = NodeClassificationPredict.memoryEstimation(
            produceProbabilities,
            batchSize,
            featureCount,
            classCount
        );

        var smallishGraphEstimation = estimation
            .estimate(GraphDimensions.of(smallGraphNodeCount), concurrency);
        var largishGraphEstimation = estimation
            .estimate(GraphDimensions.of(smallToLargeFactor * smallGraphNodeCount), concurrency);

        var smallGraphMax = smallishGraphEstimation.memoryUsage().max;
        var smallGraphMin = smallishGraphEstimation.memoryUsage().min;
        assertThat(smallGraphMax).isEqualTo(smallGraphMin);

        var largishGraphMax = largishGraphEstimation.memoryUsage().max;
        assertThat(largishGraphMax).isCloseTo(smallToLargeFactor * smallGraphMax, withPercentage(1));
    }

    @Test
    void memoryEstimationShouldBeIndifferentToConcurrency() {
        var batchSize = 1000;
        var featureCount = 500;
        var classCount = 100;
        var produceProbabilities = false;

        var nodeCount = 1_000_000;
        var lessConcurrency = new Concurrency(1);
        var moreConcurrency = new Concurrency(4);

        var estimation = NodeClassificationPredict.memoryEstimation(produceProbabilities, batchSize, featureCount, classCount);

        var lessConcurrencyEstimate = estimation
            .estimate(GraphDimensions.of(nodeCount), lessConcurrency);
        var moreConcurrencyEstimate = estimation
            .estimate(GraphDimensions.of(nodeCount), moreConcurrency);

        var lessConcurrencyMax = lessConcurrencyEstimate.memoryUsage().max;
        var lessConcurrencyMin = lessConcurrencyEstimate.memoryUsage().min;
        assertThat(lessConcurrencyMax).isEqualTo(lessConcurrencyMin);

        var moreConcurrentcyMax = moreConcurrencyEstimate.memoryUsage().max;
        assertThat(moreConcurrentcyMax).isEqualTo(lessConcurrencyMax);
    }
}
