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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfOpenHashContainer;

class GraphSageAlgorithmFactoryTest {

    @BeforeAll
    static void setup() {
        GdsEdition.instance().setToEnterpriseEdition();
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    @ParameterizedTest
    @MethodSource("parameters")
    void memoryEstimation(
        GraphSageBaseConfig gsConfig,
        long nodeCount,
        LongUnaryOperator hugeObjectArraySize
    ) {
        var trainConfig = gsConfig.trainConfig();

        // features: HugeOA[nodeCount * double[featureSize]]
        var initialFeaturesArray = sizeOfDoubleArray(gsConfig.trainConfig().featuresSize());
        var initialFeaturesMemory = hugeObjectArraySize.applyAsLong(initialFeaturesArray);

        // result: HugeOA[nodeCount * double[embeddingDimension]]
        var resultFeaturesArray = sizeOfDoubleArray(gsConfig.trainConfig().embeddingDimension());
        var resultFeaturesMemory = hugeObjectArraySize.applyAsLong(resultFeaturesArray);

        // batches:
        // per thread:

        var minBatchNodeCount = (long) gsConfig.batchSize();
        var maxBatchNodeCount = (long) gsConfig.batchSize();

        var batchSizes = new ArrayList<LongLongPair>();
        var subGraphMemories = new ArrayList<MemoryRange>();
        var layerConfigs = gsConfig.trainConfig().layerConfigs();
        for (LayerConfig layerConfig : layerConfigs) {
            var sampleSize = layerConfig.sampleSize();

            // int[3bs] selfAdjacency
            var minSelfAdjacencyMemory = sizeOfIntArray(minBatchNodeCount);
            var maxSelfAdjacencyMemory = sizeOfIntArray(maxBatchNodeCount);

            // int[3bs][0-sampleSize] adjacency
            var minAdjacencyMemory = sizeOfObjectArray(minBatchNodeCount);
            var maxAdjacencyMemory = sizeOfObjectArray(maxBatchNodeCount);

            var minInnerAdjacencyMemory = sizeOfIntArray(0);
            var maxInnerAdjacencyMemory = sizeOfIntArray(sampleSize);

            minAdjacencyMemory += minBatchNodeCount * minInnerAdjacencyMemory;
            maxAdjacencyMemory += maxBatchNodeCount * maxInnerAdjacencyMemory;

            // 3bs -> [min(3bs, nodeCount) .. min(3bs * (sampleSize + 1), nodeCount)]
            var minNextNodeCount = Math.min(minBatchNodeCount, nodeCount);
            var maxNextNodeCount = Math.min(maxBatchNodeCount * (sampleSize + 1), nodeCount);

            // nodeIds long[3bs]
            var minNextNodesMemory = sizeOfLongArray(minNextNodeCount);
            var maxNextNodesMemory = sizeOfLongArray(maxNextNodeCount);

            var minSubGraphMemory = minSelfAdjacencyMemory + minAdjacencyMemory + minNextNodesMemory;
            var maxSubGraphMemory = maxSelfAdjacencyMemory + maxAdjacencyMemory + maxNextNodesMemory;

            // LongIntHashMap toInternalId;
            // This is a local peak; will be GC'd before the global peak
            var minLocalIdMapMemory = sizeOfOpenHashContainer(minNextNodeCount) + sizeOfLongArray(minNextNodeCount);
            var maxLocalIdMapMemory = sizeOfOpenHashContainer(maxNextNodeCount) + sizeOfLongArray(maxNextNodeCount);

            subGraphMemories.add(MemoryRange.of(minSubGraphMemory, maxSubGraphMemory));
            batchSizes.add(PrimitiveTuples.pair(minBatchNodeCount, maxBatchNodeCount));

            minBatchNodeCount = minNextNodeCount;
            maxBatchNodeCount = maxNextNodeCount;
        }

        var aggregatorBatchSizes = new ArrayList<>(batchSizes);
        Collections.reverse(aggregatorBatchSizes);

        var firstLayerBatchNodeCount = aggregatorBatchSizes.get(0);

        var aggregatorNodeCounts = aggregatorBatchSizes.iterator();
        var previousAggregatorNodeCounts = Stream.concat(
            Stream.of(firstLayerBatchNodeCount),
            aggregatorBatchSizes.stream()
        ).iterator();

        var aggregatorMemories = layerConfigs.stream().map(layerConfig -> {
            var nodeCounts = aggregatorNodeCounts.next();
            var minNodeCount = nodeCounts.getOne();
            var maxNodeCount = nodeCounts.getTwo();

            var previousNodeCounts = previousAggregatorNodeCounts.next();

            long minAggregatorMemory;
            long maxAggregatorMemory;
            if (layerConfig.aggregatorType() == Aggregator.AggregatorType.MEAN) {
                var featureOrEmbeddingSize = layerConfig.cols();

                //   multi mean - new double[[..adjacency.length(=iterNodeCount)] * featureSize];
                var minMeans = sizeOfDoubleArray(minNodeCount * featureOrEmbeddingSize);
                var maxMeans = sizeOfDoubleArray(maxNodeCount * featureOrEmbeddingSize);

                //   MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingDimension]
                var minProduct = sizeOfDoubleArray(minNodeCount * trainConfig.embeddingDimension());
                var maxProduct = sizeOfDoubleArray(maxNodeCount * trainConfig.embeddingDimension());

                //   activation function = same as input
                var minActivation = minProduct;
                var maxActivation = maxProduct;

                minAggregatorMemory = minMeans + minProduct + minActivation;
                maxAggregatorMemory = maxMeans + maxProduct + maxActivation;
            } else if (layerConfig.aggregatorType() == Aggregator.AggregatorType.POOL) {
                var minPreviousNodeCount = previousNodeCounts.getOne();
                var maxPreviousNodeCount = previousNodeCounts.getTwo();

                //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount(-1) * embeddingDimension]
                var minWeightedPreviousLayer = sizeOfDoubleArray(minPreviousNodeCount * trainConfig.embeddingDimension());
                var maxWeightedPreviousLayer = sizeOfDoubleArray(maxPreviousNodeCount * trainConfig.embeddingDimension());

                // MatrixVectorSum = shape is matrix input == weightedPreviousLayer
                var minBiasedWeightedPreviousLayer = minWeightedPreviousLayer;
                var maxBiasedWeightedPreviousLayer = maxWeightedPreviousLayer;

                //   activation function = same as input
                var minNeighborhoodActivations = minBiasedWeightedPreviousLayer;
                var maxNeighborhoodActivations = maxBiasedWeightedPreviousLayer;

                //  ElementwiseMax - double[iterNodeCount * embeddingDimension]
                var minElementwiseMax = sizeOfDoubleArray(minNodeCount * trainConfig.embeddingDimension());
                var maxElementwiseMax = sizeOfDoubleArray(maxNodeCount * trainConfig.embeddingDimension());

                //  Slice - double[iterNodeCount * embeddingDimension]
                var minSelfPreviousLayer = sizeOfDoubleArray(minNodeCount * trainConfig.embeddingDimension());
                var maxSelfPreviousLayer = sizeOfDoubleArray(maxNodeCount * trainConfig.embeddingDimension());

                //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingDimension]
                var minSelf = sizeOfDoubleArray(minNodeCount * trainConfig.embeddingDimension());
                var maxSelf = sizeOfDoubleArray(maxNodeCount * trainConfig.embeddingDimension());

                //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingDimension]
                var minNeighbors = sizeOfDoubleArray(minNodeCount * trainConfig.embeddingDimension());
                var maxNeighbors = sizeOfDoubleArray(maxNodeCount * trainConfig.embeddingDimension());

                //  MatrixSum - new double[iterNodeCount * embeddingDimension]
                var minSum = minSelf;
                var maxSum = maxSelf;

                //  activation function = same as input
                var minActivation = minSum;
                var maxActivation = maxSum;

                minAggregatorMemory = minWeightedPreviousLayer + minBiasedWeightedPreviousLayer + minNeighborhoodActivations + minElementwiseMax + minSelfPreviousLayer + minSelf + minNeighbors + minSum + minActivation;
                maxAggregatorMemory = maxWeightedPreviousLayer + maxBiasedWeightedPreviousLayer + maxNeighborhoodActivations + maxElementwiseMax + maxSelfPreviousLayer + maxSelf + maxNeighbors + maxSum + maxActivation;
            } else {
                // never happens
                minAggregatorMemory = 0;
                maxAggregatorMemory = 0;
            }

            return MemoryRange.of(minAggregatorMemory, maxAggregatorMemory);
        }).collect(toList());

        // normalize rows = same as input (output of aggregator)
        var minNormalizeRows = sizeOfDoubleArray(batchSizes.get(0).getOne() * trainConfig.embeddingDimension());
        var maxNormalizeRows = sizeOfDoubleArray(batchSizes.get(0).getTwo() * trainConfig.embeddingDimension());
        aggregatorMemories.add(MemoryRange.of(minNormalizeRows, maxNormalizeRows));

        // previous layer representation = parent = local features: double[(bs..3bs) * featureSize]
        var minFirstLayerMemory = sizeOfDoubleArray(firstLayerBatchNodeCount.getOne());
        var maxFirstLayerMemory = sizeOfDoubleArray(firstLayerBatchNodeCount.getTwo());
        aggregatorMemories.add(0, MemoryRange.of(minFirstLayerMemory, maxFirstLayerMemory));

        var lossFunctionMemory = Stream.concat(
            subGraphMemories.stream(),
            aggregatorMemories.stream()
        ).reduce(MemoryRange.empty(), MemoryRange::add);

        var concurrency = gsConfig.concurrency();
        var evaluateLossMemory = lossFunctionMemory.times(concurrency);

        var expectedMemory = evaluateLossMemory
            .add(MemoryRange.of(initialFeaturesMemory))
            .add(MemoryRange.of(resultFeaturesMemory))
            .add(MemoryRange.of(40L)); // GraphSage.class

        var actualTree = new GraphSageAlgorithmFactory<>()
            .memoryEstimation(gsConfig).estimate(GraphDimensions.of(nodeCount), concurrency);

        MemoryRange actual = actualTree.memoryUsage();

        assertEquals(expectedMemory.min, actual.min);
        assertEquals(expectedMemory.max, actual.max);
    }

    static Stream<Arguments> parameters() {
        var smallNodeCounts = List.of(1L, 10L, 100L, 10_000L);
        var largeNodeCounts = List.of(11000000000L, 100000000000L);
        var nodeCounts = Stream.concat(
            smallNodeCounts.stream().map(nc -> {
                var hugeObjectArrayPages = sizeOfObjectArray(nc);
                return Tuples.pair(
                    nc,
                    (LongUnaryOperator) new LongUnaryOperator() {
                        @Override
                        public long applyAsLong(long innerSize) {
                            return 24 + hugeObjectArrayPages + nc * innerSize;
                        }

                        @Override
                        public String toString() {
                            return "single page";
                        }
                    }
                );
            }),
            largeNodeCounts.stream().map(nc -> {
                var numPages = BitUtil.ceilDiv(nc, 1L << 14);
                var hugeObjectArrayPages = sizeOfObjectArray(numPages) + numPages * sizeOfObjectArray(1L << 14);
                return Tuples.pair(
                    nc,
                    (LongUnaryOperator) new LongUnaryOperator() {
                        @Override
                        public long applyAsLong(long innerSize) {
                            return 32 + hugeObjectArrayPages + nc * innerSize;
                        }

                        @Override
                        public String toString() {
                            return "multiple pages";
                        }
                    }
                );
            })
        );

        var userName = "userName";
        var modelName = "modelName";

        var concurrencies = List.of(1, 4, 42);
        var batchSizes = List.of(1, 100, 10_000);
        var nodePropertySizes = List.of(1, 9, 42);
        var embeddingDimensions = List.of(64, 256);
        var aggregators = List.of(Aggregator.AggregatorType.MEAN, Aggregator.AggregatorType.POOL);
        var degreesAsProperty = List.of(true, false);
        var sampleSizesList = List.of(List.of(5L, 100L));

        return nodeCounts.flatMap(nodeCountPair -> {
            var nodeCount = nodeCountPair.getOne();
            var hugeObjectArraySize = nodeCountPair.getTwo();

            return concurrencies.stream().flatMap(concurrency ->
                sampleSizesList.stream().flatMap(sampleSizes ->
                    batchSizes.stream().flatMap(batchSize ->
                        aggregators.stream().flatMap(aggregator ->
                            embeddingDimensions.stream().flatMap(embeddingDimension ->
                                degreesAsProperty.stream().flatMap(degreeAsProperty ->
                                    nodePropertySizes.stream().map(nodePropertySize -> {
                                        var trainConfig = ImmutableGraphSageTrainConfig
                                            .builder()
                                            .modelName(modelName)
                                            .username(userName)
                                            .concurrency(concurrency)
                                            .sampleSizes(sampleSizes)
                                            .batchSize(batchSize)
                                            .aggregator(aggregator)
                                            .embeddingDimension(embeddingDimension)
                                            .degreeAsProperty(degreeAsProperty)
                                            .nodePropertyNames(
                                                IntStream.range(0, nodePropertySize)
                                                    .mapToObj(i -> String.valueOf('a' + i))
                                                    .collect(toList())
                                            )
                                            .build();

                                        var model = Model.of(
                                            userName,
                                            modelName,
                                            "graphSage",
                                            GraphSchema.empty(),
                                            new Layer[]{},
                                            trainConfig
                                        );

                                        ModelCatalog.set(model);

                                        var streamConfig = ImmutableGraphSageStreamConfig
                                            .builder()
                                            .concurrency(concurrency)
                                            .modelName(modelName)
                                            .username(userName)
                                            .batchSize(batchSize)
                                            .build();

                                        return arguments(
                                            streamConfig,
                                            nodeCount,
                                            hugeObjectArraySize
                                        );
                                    })
                                )
                            )
                        )
                    )
                )
            );
        });
    }

    @Test
    void mutateHasPersistentPart() {
        var modelName = "modelName";

        var trainConfig = ImmutableGraphSageTrainConfig
            .builder()
            .modelName(modelName)
            .degreeAsProperty(true)
            .build();

        var model = Model.of(
            "",
            modelName,
            "graphSage",
            GraphSchema.empty(),
            new Layer[]{},
            trainConfig
        );

        ModelCatalog.set(model);

        var config = ImmutableGraphSageMutateConfig
            .builder()
            .modelName(modelName)
            .mutateProperty("foo")
            .build();

        var actualTree = new GraphSageAlgorithmFactory<>()
            .memoryEstimation(config).estimate(GraphDimensions.of(10000), 4);

        System.out.println(actualTree.render());

        MemoryRange actual = actualTree.memoryUsage();

        assertEquals(6861816, actual.min);
        assertEquals(18356216, actual.max);

        assertThat(actualTree.persistentMemory())
            .isPresent()
            .map(MemoryTree::memoryUsage)
            .contains(MemoryRange.of(5320040L));
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }
}
