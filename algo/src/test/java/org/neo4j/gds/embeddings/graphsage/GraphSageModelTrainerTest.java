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
import org.assertj.core.data.Offset;
import org.assertj.core.util.DoubleComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfigImpl;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.AbstractVariable;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.helper.TensorTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.LongStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType;

@GdlExtension
class GraphSageModelTrainerTest {

    private final int FEATURES_COUNT = 5;
    private final int EMBEDDING_DIMENSION = 64;

    @SuppressFBWarnings("HSC_HUGE_SHARED_STRING_CONSTANT")
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
    private GraphStore graphStore;

    private Graph weightedGraph;

    private Graph unweightedGraph;
    @Inject
    private Graph arrayGraph;
    private HugeObjectArray<double[]> features;
    private GraphSageTrainConfigImpl.Builder configBuilder;


    @BeforeEach
    void setUp() {
        weightedGraph = graphStore.getUnion();
        unweightedGraph = graphStore.getGraph(graphStore.nodeLabels(), graphStore.relationshipTypes(), Optional.empty());

        long nodeCount = graphStore.nodeCount();
        features = HugeObjectArray.newArray(double[].class, nodeCount);

        Random random = new Random(19L);
        LongStream.range(0, nodeCount).forEach(n -> features.set(n, random.doubles(FEATURES_COUNT).toArray()));
        configBuilder = GraphSageTrainConfigImpl.builder()
            .modelUser("DUMMY")
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
            .aggregator(AggregatorType.MEAN)
            .modelName(MODEL_NAME)
            .build();

        var trainModel = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        var maybeWeights = useRelationshipWeight ? Optional.of("times") : Optional.<String>empty();

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(
            graphStore.getGraph(graphStore.nodeLabels(), graphStore.relationshipTypes(), maybeWeights),
            features
        );

        Layer[] layers = result.layers();
        assertThat(layers)
            .hasSize(2)
            .allSatisfy(layer -> assertThat(layer.weights())
                .hasSize(1)
                .noneMatch(weights -> TensorTestUtils.containsNaN(weights.data())));

        // First layer is (embeddingDimension x features.length)
        assertThat(layers[0].weights())
            .map(AbstractVariable::dimensions)
            .containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, FEATURES_COUNT));

        // Second layer weights (embeddingDimension x embeddingDimension)
        assertThat(layers[1].weights())
            .map(AbstractVariable::dimensions)
            .containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, EMBEDDING_DIMENSION));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void trainsWithPoolAggregator(boolean useRelationshipWeight) {
        Graph graph = unweightedGraph;
        if (useRelationshipWeight) {
            configBuilder.relationshipWeightProperty("times");
            graph = weightedGraph;
        }

        var config = configBuilder
            .aggregator(AggregatorType.POOL)
            .modelName(MODEL_NAME)
            .build();

        var trainModel = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(graph, features);
        Layer[] layers = result.layers();

        assertThat(layers)
            .hasSize(2)
            .allSatisfy(layer -> assertThat(layer.weights())
                .hasSize(4)
                .noneMatch(weights -> TensorTestUtils.containsNaN(weights.data()))
            );

        var firstWeights = layers[0].weights();
        var firstLayerPoolWeights = firstWeights.get(0).dimensions();
        var firstLayerSelfWeights = firstWeights.get(1).dimensions();
        var firstLayerNeighborsWeights = firstWeights.get(2).dimensions();
        var firstLayerBias = firstWeights.get(3).dimensions();

        assertThat(firstLayerPoolWeights).containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, FEATURES_COUNT));
        assertThat(firstLayerSelfWeights).containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, FEATURES_COUNT));
        assertThat(firstLayerNeighborsWeights).containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, EMBEDDING_DIMENSION));
        assertThat(firstLayerBias).containsExactly(Dimensions.vector(EMBEDDING_DIMENSION));

        var secondWeights = layers[1].weights();
        var secondLayerPoolWeights = secondWeights.get(0).dimensions();
        var secondLayerSelfWeights = secondWeights.get(1).dimensions();
        var secondLayerNeighborsWeights = secondWeights.get(2).dimensions();
        var secondLayerBias = secondWeights.get(3).dimensions();

        assertThat(secondLayerPoolWeights).containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, EMBEDDING_DIMENSION));
        assertThat(secondLayerSelfWeights).containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, EMBEDDING_DIMENSION));
        assertThat(secondLayerNeighborsWeights).containsExactly(Dimensions.matrix(EMBEDDING_DIMENSION, EMBEDDING_DIMENSION));
        assertThat(secondLayerBias).containsExactly(Dimensions.vector(EMBEDDING_DIMENSION));
    }

    @Test
    void shouldTrainModelWithArrayProperties() {
        var arrayFeatures = HugeObjectArray.newArray(double[].class, arrayGraph.nodeCount());
        LongStream
            .range(0, arrayGraph.nodeCount())
            .forEach(n -> arrayFeatures.set(n, arrayGraph.nodeProperties("features").doubleArrayValue(n)));
        var config = GraphSageTrainConfig.testBuilder()
            .embeddingDimension(12)
            .aggregator(AggregatorType.MEAN)
            .activationFunction(ActivationFunction.SIGMOID)
            .addFeatureProperty("features")
            .modelName("model")
            .build();

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        var result = trainer.train(arrayGraph, arrayFeatures);

        assertThat(result.layers())
            .allSatisfy(layer -> assertThat(layer.weights())
                .noneMatch(weights -> TensorTestUtils.containsNaN(weights.data()))
            );
    }

    @Test
    void testLosses() {
        var config = configBuilder
            .modelName("randomSeed2")
            .embeddingDimension(12)
            .epochs(10)
            .tolerance(1e-10)
            .sampleSizes(List.of(5, 3))
            .batchSize(5)
            .maxIterations(100)
            .randomSeed(42L)
            .build();

        var trainer = new GraphSageModelTrainer(
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var trainResult = trainer.train(unweightedGraph, features);

        var metrics = trainResult.metrics();
        assertThat(metrics.didConverge()).isFalse();
        assertThat(metrics.ranEpochs()).isEqualTo(10);
        assertThat(metrics.ranIterationsPerEpoch()).containsExactly(100, 100, 100, 100, 100, 100, 100, 100, 100, 100);

        assertThat(metrics.epochLosses().stream().mapToDouble(Double::doubleValue).toArray())
            .contains(new double[]{
                18.25,
                16.31,
                16.41,
                16.21,
                14.96,
                14.97,
                14.31,
                16.17,
                14.90,
                15.58
                }, Offset.offset(0.05)
            );
    }

    @Test
    void testLossesWithPoolAggregator() {
        var config = configBuilder
            .modelName("randomSeed2")
            .embeddingDimension(12)
            .aggregator(AggregatorType.POOL)
            .epochs(10)
            .tolerance(1e-10)
            .sampleSizes(List.of(5, 3))
            .batchSize(5)
            .penaltyL2(0.01)
            .maxIterations(10)
            .randomSeed(42L)
            .build();

        var trainer = new GraphSageModelTrainer(
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var trainResult = trainer.train(unweightedGraph, features);

        var metrics = trainResult.metrics();
        assertThat(metrics.didConverge()).isFalse();
        assertThat(metrics.ranEpochs()).isEqualTo(10);
        assertThat(metrics.ranIterationsPerEpoch()).containsOnly(10);

        assertThat(metrics.epochLosses().stream().mapToDouble(Double::doubleValue).toArray())
            .contains(new double[]{
                23.41,
                19.94,
                19.70,
                21.62,
                19.06,
                24.11,
                19.72,
                16.47,
                19.74,
                20.97
                }, Offset.offset(0.05)
            );
    }

    @Test
    void testConvergence() {
        var trainer = new GraphSageModelTrainer(
            configBuilder.modelName("convergingModel:)").tolerance(100.0).epochs(10).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var trainResult = trainer.train(unweightedGraph, features);

        var trainMetrics = trainResult.metrics();
        assertThat(trainMetrics.didConverge()).isTrue();
        assertThat(trainMetrics.ranEpochs()).isEqualTo(1);
        assertThat(trainMetrics.ranIterationsPerEpoch()).containsExactly(2);
    }

    @Test
    void batchesPerIteration() {
        configBuilder.modelName("convergingModel:)")
            .embeddingDimension(2)
            .aggregator(AggregatorType.POOL)
            .epochs(10)
            .tolerance(1e-5)
            .sampleSizes(List.of(1))
            .batchSize(5)
            .maxIterations(100)
            .randomSeed(42L);

        var trainResultWithoutSampling = new GraphSageModelTrainer(
            configBuilder.maybeBatchSamplingRatio(1.0).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).train(unweightedGraph, features);

        var trainResultWithSampling = new GraphSageModelTrainer(
            configBuilder.maybeBatchSamplingRatio(0.01).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).train(unweightedGraph, features);

        // reason: sampling results in more stochastic gradient descent and different losses
        assertThat(trainResultWithoutSampling.metrics().epochLosses().get(0)).isNotEqualTo(trainResultWithSampling.metrics().epochLosses().get(0));
    }

    @ParameterizedTest
    @CsvSource(value = {"0.01, 26.6", "0.5, 27.4", "1, 28.20"})
    void l2Penalty(double penalty, double expectedLoss) {
        var config = this.configBuilder
            .modelName("penaltyTest")
            .embeddingDimension(12)
            .epochs(1)
            .maxIterations(1)
            .tolerance(1e-10)
            .sampleSizes(List.of(5, 3))
            .penaltyL2(penalty)
            .batchSize(5)
            .randomSeed(42L)
            .build();

        var result = new GraphSageModelTrainer(
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).train(unweightedGraph, features);

        Offset<Double> offset = Offset.offset(0.05);
        assertThat((result.metrics().iterationLossPerEpoch().get(0).get(0))).isEqualTo(expectedLoss, offset);
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

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        var otherTrainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        var result = trainer.train(unweightedGraph, features);
        var otherResult = otherTrainer.train(unweightedGraph, features);

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

        var trainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);
        var otherTrainer = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        var result = trainer.train(unweightedGraph, features);
        var otherResult = otherTrainer.train(unweightedGraph, features);

        // Needs deterministic weights updates
        assertThat(result).usingRecursiveComparison().withComparatorForType(new DoubleComparator(1e-10), Double.class).isEqualTo(otherResult);
    }
}
