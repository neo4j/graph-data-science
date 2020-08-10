/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.randomprojections.ImmutableRandomProjectionBaseConfig;
import org.neo4j.gds.embeddings.randomprojections.RandomProjection;
import org.neo4j.gds.embeddings.randomprojections.RandomProjectionBaseConfig;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.graphalgo.TestSupport.crossArguments;

class GraphSageModelTrainEmbedTest extends BaseProcTest {

    private static final int FEATURES_PER_NODE = 128;
    private static final int GS_DIM = 64;

    private Graph graph;
    private HugeObjectArray<double[]> features;
    private Layer layer2;
    private Layer layer1;
    private Log log;

    @BeforeEach
    void setUp() {
        log = new TestLog();

        createRandomGraph(100, 5);

        long nodeCount = graph.nodeCount();
        features = HugeObjectArray.newArray(double[].class, nodeCount, AllocationTracker.EMPTY);
        RandomProjectionBaseConfig config = ImmutableRandomProjectionBaseConfig.builder()
            .embeddingSize(FEATURES_PER_NODE)
            .maxIterations(4)
            .normalizeL2(true)
            .addAllIterationWeights(List.of(1.0, 1.0, 7.81, 45.28))
            .normalizationStrength(-0.628f)
            .build();

        var progressLogger = new BatchingProgressLogger(NullLog.getInstance(), 0, "Test", 1);
        RandomProjection randomProjection = new RandomProjection(
            graph,
            config,
            progressLogger,
            AllocationTracker.EMPTY
        );

        var randomProjections = randomProjection.compute().embeddings();
        LongStream.range(0, nodeCount).forEach(n -> {
            double[] doubleFeatures = IntStream.range(0, FEATURES_PER_NODE)
                .mapToDouble(i -> randomProjections.get(n)[i]).toArray();
            features.set(n, doubleFeatures);
        });

        normalizeFeatures(nodeCount);
    }

    private void createRandomGraph(int nodeCount, int averageDegree) {
        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator.builder()
            .nodeCount(nodeCount)
            .averageDegree(averageDegree)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .allocationTracker(AllocationTracker.EMPTY)
            .build();
        graph = randomGraphGenerator.generate();
    }

    private void setupLayers(ActivationFunction activationFunction, AggregatorType aggregatorType) {
        LayerConfig layer1Config = LayerConfig.builder()
            .aggregatorType(aggregatorType)
            .rows(GS_DIM)
            .cols(FEATURES_PER_NODE)
            .sampleSize(20)
            .activationFunction(activationFunction)
            .build();

        LayerConfig layer2Config = LayerConfig.builder()
            .aggregatorType(aggregatorType)
            .rows(GS_DIM)
            .cols(GS_DIM)
            .sampleSize(25)
            .activationFunction(activationFunction)
            .build();

        layer1 = LayerFactory.createLayer(layer1Config);
        layer2 = LayerFactory.createLayer(layer2Config);

    }

    @ParameterizedTest
    @MethodSource("configVariations")
    void smokeTestTraining(int concurrency, AggregatorType aggregator, ActivationFunction activationFunction) {
        setupLayers(activationFunction, aggregator);
        GraphSageModel model = new GraphSageModel(
            concurrency,
            (int) (graph.nodeCount() / 20),
            List.of(layer1, layer2),
            log
        );
        runSmokeTest(model);
    }

    @ParameterizedTest
    @MethodSource("configVariations")
    void smokeTestTrainingSingleBatch(int concurrency, AggregatorType aggregator, ActivationFunction activationFunction) {
        setupLayers(activationFunction, aggregator);
        GraphSageModel model = new GraphSageModel(concurrency, (int) graph.nodeCount(), List.of(layer1, layer2), log);
        runSmokeTest(model);
    }

    void runSmokeTest(GraphSageModel model) {
        Stream<? extends Tensor<?>> layer2Stream = layer2.weights()
            .stream().map(Weights::data).map(Tensor::copy);
        List<Tensor<?>> weightsBeforeTraining = layer2Stream
            .collect(Collectors.toList());

        model.train(graph, features);

        assertNotNull(layer1);
        Stream<? extends Tensor<?>> layer1Stream = layer1.weights()
            .stream().map(Weights::data).map(Tensor::copy);
        List<Tensor<?>> expectedWeights = layer1Stream
            .collect(Collectors.toList());

        for (int i = 0; i < expectedWeights.size(); i++) {
            assertThat(weightsBeforeTraining.get(i).data(), not(IsEqual.equalTo(expectedWeights.get(i).data())));
        }
    }

    static Stream<Arguments> configVariations() {
        return crossArguments(
            () -> Stream.of(
                Arguments.of(1),
                Arguments.of(4)
            ),
            () -> Stream.of(
                Arguments.of(AggregatorType.MEAN),
                Arguments.of(AggregatorType.POOL)
            ),
            () -> Stream.of(
                Arguments.of(ActivationFunction.SIGMOID),
                Arguments.of(ActivationFunction.RELU)
            )
        );
    }

    // TODO: Should this be production code and pre-processing step for the algorithm?
    private void normalizeFeatures(long nodeCount) {

        double[] avgFeatures = new double[FEATURES_PER_NODE];
        double[] stdFeatures = new double[FEATURES_PER_NODE];

        LongStream.range(0, nodeCount).forEach(n -> {
            double[] doubleFeatures = features.get(n);

            IntStream.range(0, FEATURES_PER_NODE).forEach(f ->
                avgFeatures[f] += doubleFeatures[f] / nodeCount
            );
        });

        LongStream.range(0, nodeCount).forEach(n -> {
            double[] doubles = features.get(n);

            IntStream.range(0, FEATURES_PER_NODE).forEach(f -> {
                doubles[f] -= avgFeatures[f];
                stdFeatures[f] += Math.pow(doubles[f], 2) / (nodeCount - 1);
            });

            features.set(n, doubles);
        });

        LongStream.range(0, nodeCount).forEach(n -> {
            double[] doubles = features.get(n);

            IntStream.range(0, FEATURES_PER_NODE).forEach(f -> {
                doubles[f] /= Math.sqrt(stdFeatures[f]);
            });

            features.set(n, doubles);
        });
    }
}
