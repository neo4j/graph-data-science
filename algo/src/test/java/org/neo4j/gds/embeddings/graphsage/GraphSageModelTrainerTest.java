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
package org.neo4j.gds.embeddings.graphsage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.ml.functions.Weights;
import org.neo4j.gds.core.ml.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.logging.NullLog;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;

@GdlExtension
class GraphSageModelTrainerTest {

    private final int FEATURES_COUNT = 5;
    private final int EMBEDDING_DIMENSION = 64;

    @SuppressFBWarnings
    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @SuppressFBWarnings
    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "array")
    private static final String ARRAY_GRAPH = "CREATE" +
                                              "  (a { features: [-1.0, 2.1] })" +
                                              ", (b { features: [4.2, -1.6] })" +
                                              ", (a)-[:REL]->(b)";

    private final String MODEL_NAME = "graphSageModel";

    @Inject
    private Graph graph;
    @Inject
    private Graph arrayGraph;
    private HugeObjectArray<double[]> features;
    private ImmutableGraphSageTrainConfig.Builder configBuilder;


    @BeforeEach
    void setUp() {
        long nodeCount = graph.nodeCount();
        features = HugeObjectArray.newArray(double[].class, nodeCount, AllocationTracker.empty());

        Random random = new Random();
        LongStream.range(0, nodeCount).forEach(n -> features.set(n, random.doubles(FEATURES_COUNT).toArray()));
        configBuilder = ImmutableGraphSageTrainConfig.builder()
            .featureProperties(Collections.nCopies(FEATURES_COUNT, "dummyProp"))
            .embeddingDimension(EMBEDDING_DIMENSION);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void trainsWithMeanAggregator(boolean useRelationshipWeight) {
        if (useRelationshipWeight) {
            configBuilder.relationshipWeightProperty("times");
        }
        var config = configBuilder
            .aggregator(Aggregator.AggregatorType.MEAN)
            .modelName(MODEL_NAME)
            .build();

        var trainModel = new GraphSageModelTrainer(config, ProgressLogger.NULL_LOGGER);

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(graph, features);

        Layer[] layers = result.layers();
        assertEquals(2, layers.length);
        Layer first = layers[0];
        List<Weights<? extends Tensor<?>>> firstWeights = first.weights();
        assertEquals(1, firstWeights.size());

        // First layer is (embeddingDimension x features.length)
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, FEATURES_COUNT}, firstWeights.get(0).dimensions());
        Layer second = layers[1];
        List<Weights<? extends Tensor<?>>> secondWeights = second.weights();
        assertEquals(1, secondWeights.size());

        // Second layer weights (embeddingDimension x embeddingDimension)
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, EMBEDDING_DIMENSION}, secondWeights.get(0).dimensions());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void trainsWithPoolAggregator(boolean useRelationshipWeight) {
        if (useRelationshipWeight) {
            configBuilder.relationshipWeightProperty("times");
        }
        var config = configBuilder
            .aggregator(Aggregator.AggregatorType.POOL)
            .modelName(MODEL_NAME)
            .build();

        var trainModel = new GraphSageModelTrainer(config, ProgressLogger.NULL_LOGGER);

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(graph, features);
        Layer[] layers = result.layers();
        assertEquals(2, layers.length);

        Layer first = layers[0];
        List<Weights<? extends Tensor<?>>> firstWeights = first.weights();
        assertEquals(4, firstWeights.size());

        var firstLayerPoolWeights = firstWeights.get(0).dimensions();
        var firstLayerSelfWeights = firstWeights.get(1).dimensions();
        var firstLayerNeighborsWeights = firstWeights.get(2).dimensions();
        var firstLayerBias = firstWeights.get(3).dimensions();
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, FEATURES_COUNT}, firstLayerPoolWeights);
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, FEATURES_COUNT}, firstLayerSelfWeights);
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, EMBEDDING_DIMENSION}, firstLayerNeighborsWeights);
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION}, firstLayerBias);

        Layer second = layers[1];
        List<Weights<? extends Tensor<?>>> secondWeights = second.weights();
        assertEquals(4, secondWeights.size());

        var secondLayerPoolWeights = secondWeights.get(0).dimensions();
        var secondLayerSelfWeights = secondWeights.get(1).dimensions();
        var secondLayerNeighborsWeights = secondWeights.get(2).dimensions();
        var secondLayerBias = secondWeights.get(3).dimensions();
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, EMBEDDING_DIMENSION}, secondLayerPoolWeights);
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, EMBEDDING_DIMENSION}, secondLayerSelfWeights);
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION, EMBEDDING_DIMENSION}, secondLayerNeighborsWeights);
        assertArrayEquals(new int[]{EMBEDDING_DIMENSION}, secondLayerBias);
    }

    @Test
    void testLogging() {
        var config = ImmutableGraphSageTrainConfig.builder()
            .degreeAsProperty(true)
            .embeddingDimension(EMBEDDING_DIMENSION)
            .modelName("model")
            .epochs(1)
            .maxIterations(1)
            .build();

        var algo = new GraphSageTrainAlgorithmFactory(TestProgressLogger.FACTORY).build(
            graph,
            config,
            AllocationTracker.empty(),
            NullLog.getInstance(),
            EmptyProgressEventTracker.INSTANCE
        );
        algo.compute();

        var messagesInOrder = ((TestProgressLogger) algo.getProgressLogger()).getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .containsExactly(
                "GraphSageTrain :: Start",
                "GraphSageTrain :: Epoch 1 :: Start",
                "GraphSageTrain :: Iteration 1 :: Start",
                "GraphSageTrain :: Iteration 1 :: Finished",
                "GraphSageTrain :: Epoch 1 :: Finished",
                "GraphSageTrain :: Finished"
            );
    }

    @Test
    void shouldTrainModelWithArrayProperties() {
        var arrayFeatures = HugeObjectArray.newArray(double[].class, arrayGraph.nodeCount(), AllocationTracker.empty());
        LongStream
            .range(0, arrayGraph.nodeCount())
            .forEach(n -> arrayFeatures.set(n, arrayGraph.nodeProperties("features").doubleArrayValue(n)));
        var config = GraphSageTrainConfig.builder()
            .embeddingDimension(12)
            .aggregator(Aggregator.AggregatorType.MEAN)
            .activationFunction(ActivationFunction.SIGMOID)
            .addFeatureProperty("features")
            .modelName("model")
            .build();

        var trainer = new GraphSageModelTrainer(config, ProgressLogger.NULL_LOGGER);

        trainer.train(arrayGraph, arrayFeatures);
    }
}
