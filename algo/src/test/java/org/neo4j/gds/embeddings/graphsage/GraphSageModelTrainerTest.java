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

import com.carrotsearch.hppc.LongHashSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.assertj.core.util.DoubleComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.graphalgo.core.utils.progress.EmptyProgressEventTracker;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.logging.NullLog;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;
import static org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph.DUMMY_PROPERTY;

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

        Random random = new Random(19L);
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

        var trainModel = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

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

        var trainModel = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

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
            .addFeatureProperties(DUMMY_PROPERTY)
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

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

        trainer.train(arrayGraph, arrayFeatures);
    }

    @RepeatedTest(value = 25, name = RepeatedTest.LONG_DISPLAY_NAME)
    void testLosses() {
        var config = configBuilder
            .modelName("randomSeed2")
            .embeddingDimension(12)
            .epochs(10)
            .tolerance(1e-10)
            .addSampleSizes(5, 3)
            .batchSize(5)
            .maxIterations(100)
            .randomSeed(42L)
            .build();

        var trainer = new GraphSageModelTrainer(
            config,
            Pools.DEFAULT,
            ProgressLogger.NULL_LOGGER
        );

        var trainResult = trainer.train(graph, features);

        var metrics = trainResult.metrics();
        assertThat(metrics.didConverge()).isFalse();
        assertThat(metrics.ranEpochs()).isEqualTo(10);

        var metricsMap =  metrics.toMap().get("metrics");
        assertThat(metricsMap).isInstanceOf(Map.class);

        var epochLosses = ((Map<String, Object>) metricsMap).get("epochLosses");
        assertThat(epochLosses)
            .isInstanceOf(List.class)
            .asList()
            .containsExactly(
                89.89507983622258,
                84.71357307588974,
                84.18589853328612,
                84.10179444143131,
                84.07430575768018,
                80.97075806917714,
                80.96268919745964,
                81.02599920488873,
                80.99105515654075,
                80.95808277983977
            );
    }

    @Test
    void testConvergence() {
        var trainer = new GraphSageModelTrainer(
            configBuilder.modelName("convergingModel:)").tolerance(100.0).epochs(10).build(),
            Pools.DEFAULT,
            ProgressLogger.NULL_LOGGER
        );

        var trainResult = trainer.train(graph, features);

        var trainMetrics = trainResult.metrics();
        assertThat(trainMetrics.didConverge()).isTrue();
        assertThat(trainMetrics.ranEpochs()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(longs = {20L, -100L, 30L})
    void seededSingleBatch(long seed) {
        var config = configBuilder
            .modelName("randomSeed")
            .embeddingDimension(12)
            .randomSeed(seed)
            .concurrency(1)
            .build();

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);
        var otherTrainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

        var result = trainer.train(graph, features);
        var otherResult = otherTrainer.train(graph, features);

        assertThat(result).usingRecursiveComparison().isEqualTo(otherResult);
    }

    @ParameterizedTest
    @ValueSource(longs = {20L, -100L, 30L})
    void seededMultiBatch(long seed) {
        var config = configBuilder
            .modelName("randomSeed")
            .embeddingDimension(12)
            .randomSeed(seed)
            .concurrency(1)
            .batchSize(5)
            .build();

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);
        var otherTrainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

        var result = trainer.train(graph, features);
        var otherResult = otherTrainer.train(graph, features);

        // Needs deterministic weights updates
        assertThat(result).usingRecursiveComparison().withComparatorForType(new DoubleComparator(1e-10), Double.class).isEqualTo(otherResult);
    }

    @Test
    void seededNeighborBatch() {
        var batchSize = 5;
        var seed = 20L;
        var config = configBuilder
            .modelName("randomSeed")
            .embeddingDimension(12)
            .randomSeed(seed)
            .batchSize(batchSize)
            .build();

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);
        var otherTrainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

        var partitions = PartitionUtils.rangePartitionWithBatchSize(
            graph.nodeCount(),
            batchSize,
            Function.identity()
        );

        for (int i = 0; i < partitions.size(); i++) {
            var localSeed = i + seed;
            var neighborBatch = trainer.neighborBatch(graph, partitions.get(i), localSeed);
            var otherNeighborBatch = otherTrainer.neighborBatch(graph, partitions.get(i), localSeed);
            assertThat(neighborBatch).containsExactlyElementsOf(otherNeighborBatch.boxed().collect(Collectors.toList()));
        }
    }

    @Test
    void seededNegativeBatch() {
        var batchSize = 5;
        var seed = 20L;
        var config = configBuilder
            .modelName("randomSeed")
            .embeddingDimension(12)
            .randomSeed(seed)
            .batchSize(batchSize)
            .build();

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);
        var otherTrainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressLogger.NULL_LOGGER);

        var partitions = PartitionUtils.rangePartitionWithBatchSize(
            graph.nodeCount(),
            batchSize,
            Function.identity()
        );

        var neighborsSet = new LongHashSet(5);
        neighborsSet.addAll(0, 3, 5, 6, 10);

        for (int i = 0; i < partitions.size(); i++) {
            var localSeed = i + seed;
            var negativeBatch = trainer.negativeBatch(graph, Math.toIntExact(partitions.get(i).nodeCount()), neighborsSet, localSeed);
            var otherNegativeBatch = otherTrainer.negativeBatch(graph, Math.toIntExact(partitions.get(i).nodeCount()), neighborsSet, localSeed);

            assertThat(negativeBatch).containsExactlyElementsOf(otherNegativeBatch.boxed().collect(Collectors.toList()));
        }
    }
}
