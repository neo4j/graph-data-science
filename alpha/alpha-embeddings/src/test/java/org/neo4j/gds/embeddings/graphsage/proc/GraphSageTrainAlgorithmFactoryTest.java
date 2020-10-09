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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.GraphDimensions;
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

class GraphSageTrainAlgorithmFactoryTest {

    @SuppressWarnings("UnnecessaryLocalVariable")
    @ParameterizedTest
    @MethodSource("parameters")
    void memoryEstimation(
        GraphSageTrainConfig config,
        long nodeCount,
        LongUnaryOperator hugeObjectArraySize
    ) {
        GdsEdition.instance().setToEnterpriseEdition();
        var concurrency = config.concurrency();
        var layerConfigs = config.layerConfigs();

        // initial layers
        // rows - embeddingSize, cols - feature size (first layer), embedding size every other layer
        //  applies to the weights
        //  weights = double[rows * cols], 1x for mean, 2x for maxpooling, double[row * row] for maxpooling, double[rows] bias for maxpooling
        var layersMemory = layerConfigs.stream().mapToLong(layerConfig -> {
            var weightDimensions = layerConfig.rows() * layerConfig.cols();
            var weightsMemory = sizeOfDoubleArray(weightDimensions);
            Aggregator.AggregatorType aggregatorType = layerConfig.aggregatorType();
            if (aggregatorType == Aggregator.AggregatorType.POOL) {
                // selfWeights
                weightsMemory += sizeOfDoubleArray(layerConfig.rows() * layerConfig.rows());
                // neighborsWeights
                weightsMemory += sizeOfDoubleArray(layerConfig.rows() * layerConfig.rows());
                // bias
                weightsMemory += sizeOfDoubleArray(layerConfig.rows());
            }
            return weightsMemory;
        }).sum();

        // features: HugeOA[nodeCount * double[featureSize]]
        var initialFeaturesArray = sizeOfDoubleArray(config.featuresSize());
        var initialFeaturesMemory = hugeObjectArraySize.applyAsLong(initialFeaturesArray);

        var batchSize = config.batchSize();
        var totalBatchSize = 3 * batchSize;

        // embeddings
        // subgraphs

        var minBatchNodeCount = (long) totalBatchSize;
        var maxBatchNodeCount = (long) totalBatchSize;

        var batchSizes = new ArrayList<LongLongPair>();
        var subGraphMemories = new ArrayList<MemoryRange>();
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
            // TODO somehow add those
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

                //   MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
                var minProduct = sizeOfDoubleArray(minNodeCount * config.embeddingSize());
                var maxProduct = sizeOfDoubleArray(maxNodeCount * config.embeddingSize());

                //   activation function = same as input
                var minActivation = minProduct;
                var maxActivation = maxProduct;

                minAggregatorMemory = minMeans + minProduct + minActivation;
                maxAggregatorMemory = maxMeans + maxProduct + maxActivation;
            } else if (layerConfig.aggregatorType() == Aggregator.AggregatorType.POOL) {
                var minPreviousNodeCount = previousNodeCounts.getOne();
                var maxPreviousNodeCount = previousNodeCounts.getTwo();

                //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount(-1) * embeddingSize]
                var minWeightedPreviousLayer = sizeOfDoubleArray(minPreviousNodeCount * config.embeddingSize());
                var maxWeightedPreviousLayer = sizeOfDoubleArray(maxPreviousNodeCount * config.embeddingSize());

                // MatrixVectorSum = shape is matrix input == weightedPreviousLayer
                var minBiasedWeightedPreviousLayer = minWeightedPreviousLayer;
                var maxBiasedWeightedPreviousLayer = maxWeightedPreviousLayer;

                //   activation function = same as input
                var minNeighborhoodActivations = minBiasedWeightedPreviousLayer;
                var maxNeighborhoodActivations = maxBiasedWeightedPreviousLayer;

                //  ElementwiseMax - double[iterNodeCount * embeddingSize]
                var minElementwiseMax = sizeOfDoubleArray(minNodeCount * config.embeddingSize());
                var maxElementwiseMax = sizeOfDoubleArray(maxNodeCount * config.embeddingSize());

                //  Slice - double[iterNodeCount * embeddingSize]
                var minSelfPreviousLayer = sizeOfDoubleArray(minNodeCount * config.embeddingSize());
                var maxSelfPreviousLayer = sizeOfDoubleArray(maxNodeCount * config.embeddingSize());

                //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
                var minSelf = sizeOfDoubleArray(minNodeCount * config.embeddingSize());
                var maxSelf = sizeOfDoubleArray(maxNodeCount * config.embeddingSize());

                //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
                var minNeighbors = sizeOfDoubleArray(minNodeCount * config.embeddingSize());
                var maxNeighbors = sizeOfDoubleArray(maxNodeCount * config.embeddingSize());

                //  MatrixSum - new double[iterNodeCount * embeddingSize]
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
        var minNormalizeRows = sizeOfDoubleArray(batchSizes.get(0).getOne() * config.embeddingSize());
        var maxNormalizeRows = sizeOfDoubleArray(batchSizes.get(0).getTwo() * config.embeddingSize());
        aggregatorMemories.add(MemoryRange.of(minNormalizeRows, maxNormalizeRows));

        // previous layer representation = parent = local features: double[(bs..3bs) * featureSize]
        var minFirstLayerMemory = sizeOfDoubleArray(firstLayerBatchNodeCount.getOne());
        var maxFirstLayerMemory = sizeOfDoubleArray(firstLayerBatchNodeCount.getTwo());
        aggregatorMemories.add(0, MemoryRange.of(minFirstLayerMemory, maxFirstLayerMemory));

        var lossFunctionMemory = Stream.concat(
            subGraphMemories.stream(),
            aggregatorMemories.stream()
        ).reduce(MemoryRange.empty(), MemoryRange::add);

        var evaluateLossMemory = lossFunctionMemory.times(concurrency);

        // adam optimizer
        //  copy of weight for every layer
        var initialAdamMemory = layerConfigs.stream().mapToLong(layerConfig -> {
            var weightDimensions = layerConfig.rows() * layerConfig.cols();
            var momentumTermsMemory = sizeOfDoubleArray(weightDimensions);
            var velocityTermsMemory = momentumTermsMemory;

            return momentumTermsMemory + velocityTermsMemory;
        }).sum();

        var updateAdamMemory = layerConfigs.stream().mapToLong(layerConfig -> {
            var weightDimensions = layerConfig.rows() * layerConfig.cols();
            // adam update

            //  new momentum
            //   1 copy of weights
            //   1 copy of momentum terms (same dim as weights)
            // not part of peak usage as update peak is larger
            var newMomentum = 2 * weightDimensions;

            //  new velocity
            //   2 copies of weights
            //   1 copy of momentum terms (same dim as weights)
            // not part of peak usage as update peak is larger
            var newVelocity = 3 * weightDimensions;

            //  mCaps
            //   copy of momentumTerm (size of weights)
            var mCaps = weightDimensions;

            //  vCaps
            //   copy of velocityTerm (size of weights)
            var vCaps = weightDimensions;

            //  updating weights
            //   2 copies of mCap, 1 copy of vCap
            var updateWeights = 2 * mCaps + vCaps;

            var updateMemory = updateWeights + mCaps + vCaps;

            return updateMemory;
        }).sum();

        var backwardsLossFunctionMemory =
            aggregatorMemories.stream().reduce(MemoryRange.empty(), MemoryRange::add);

        var trainOnBatchMemory =
            lossFunctionMemory
                .add(backwardsLossFunctionMemory)
                .add(MemoryRange.of(updateAdamMemory));

        var trainOnEpoch = trainOnBatchMemory
            .times(concurrency)
            .add(MemoryRange.of(initialAdamMemory));

        var trainMemory =
            trainOnEpoch
                .max(evaluateLossMemory)
                .add(MemoryRange.of(initialFeaturesMemory));

        var expectedPersistentMemory = MemoryRange.of(layersMemory);
        var expectedPeakMemory = trainMemory
            .add(expectedPersistentMemory)
            .add(MemoryRange.of(40L)); // For GraphSage.class

        var actualEstimation = new GraphSageTrainAlgorithmFactory()
            .memoryEstimation(config)
            .estimate(GraphDimensions.of(nodeCount), concurrency);

        assertEquals(expectedPeakMemory, actualEstimation.memoryUsage());
        assertThat(actualEstimation
            .components()
            .stream()
            .filter(component -> component.description().equals("persistentMemory"))
            .findFirst()
        ).isPresent().map(MemoryTree::memoryUsage).contains(expectedPersistentMemory);

        // train epoch


        // train on batch
        // times concurrency: *

        // loss function
        // 3 times batch size = 3bs

        // embeddings: start
        //  subgraph, batchSizes * sampleSize[i]


        // weights - new double[featureSize * embeddingSize] // for any following iteration it's embeddingSize * embeddingSize

        // iteration
        // iterNodeCount = starting at 3bs * sampleSize.0 * sampleSize.1
        //   this should be the largest node count in the first iteration
        // after mean = 3bs * sampleSize.0 (removing sampleSize.1)

        // mean aggregator
        //   multi mean - new double[[..adjacency.length(=iterNodeCount)] * featureSize];
        //   MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
        //   activation function = same as input
        //     rows = means.rows = iterNodeCount, cols = weights.cols = embeddingSize

        // example
        // mean aggregator next layer
        //   multi mean - new double[iterCount * embeddingSize];
        //   MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
        //   activation function = same as input
        //     rows = means.rows = iterNodeCount, cols = weights.cols = embeddingSize


        // max pooling
        //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount(-1) * embeddingSize]
        //  MatrixVectorSum = shape is matrix input == weightedPreviousLayer
        //  activation function = same as input
        //  ElementwiseMax - double[iterNodeCount * embeddingSize]
        //
        //  Slice - double[iterNodeCount * embeddingSize]
        //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
        //  MatrixMultiplyWithTransposedSecondOperand - new double[iterNodeCount * embeddingSize]
        //  MatrixSum - new double[iterNodeCount * embeddingSize]
        //  activation function = same as input


        // normalize rows = same as input (output of aggregator)

        // embeddings: end


        // SubGraph: (times layerSize)
        // nodeIds long[3bs]
        // private final LongArrayList originalIds;
        // private final LongIntHashMap toInternalId;
        // int[3bs][0-sampleSize] adjacency
        // int[3bs] selfAdjacency

        // SubGraph per Layer


        // evaluate loss
        //     ComputationContext ctx = new ComputationContext();
        //     Variable<Scalar> loss = lossFunction(batch, graph, features);
        //     doubleAdder.add(ctx.forward(loss).dataAt(0));  --> what we did above

        // train on batch

        //  ComputationContext localCtx = new ComputationContext();
        //  ->   newLoss = localCtx.forward(lossFunction).dataAt(0);  -> what we did above
        //  ->   localCtx.backward(lossFunction); -> should be same size
        //  ->   updater.update(localCtx);

        // adam update
        //  new momentum
        //   copies of weights (per layer)
        //   copies of momentum terms (per layer) (same dim as weights)
        //  new velocity
        //   2 copies of weights (per layer)
        //   copies of momentum terms (per layer) (same dim as weights)
        //  mCaps
        //   copy of momentumTerm (size of weights)
        //  vCaps
        //   copy of velocityTerm (size of weights)
        //  updating weights
        //   2 copies of mCap, 1 copy of vCap


        //        int batchSize = 100;
//        var userName = "userName";
//        var modelName = "modelName";
//        int featureSize = numberOfProperties + (degreeAsProperty ? 1 : 0);

//        var model = Model.of(
//            userName,
//            modelName,
//            "graphSage",
//            GraphSchema.empty(),
//            new Layer[]{},
//            trainConfig
//        );
//
//        ModelCatalog.set(model);
//
//        var streamConfig = ImmutableGraphSageStreamConfig
//            .builder()
//            .modelName(modelName)
//            .username(userName)
//            .batchSize(batchSize)
//            .build();
//
//        MemoryEstimation estimation = new GraphSageAlgorithmFactory<GraphSageStreamConfig>()
//            .memoryEstimation(streamConfig);
//
//        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
//
//        MemoryTree estimate = estimation.estimate(dimensions, concurrency);
//        MemoryRange actual = estimate.memoryUsage();
//
//        long doubleArray = sizeOfDoubleArray(featureSize);
//        long hugeObjectArray = hugeObjectArraySize.applyAsLong(doubleArray);
//        long perThreadArray = sizeOfDoubleArray(batchSize * featureSize);
//        long instanceSize = 40;
//        long expected = instanceSize + 2 * hugeObjectArray + concurrency * perThreadArray;
//
//        assertEquals(expected, actual.min);
//        assertEquals(expected, actual.max);
    }

    static Stream<Arguments> parameters() {
        var smallNodeCounts = List.of(1L, 100L, 10_000L);
        var largeNodeCounts = List.of(11_000_000_000L, 100_000_000_000L);
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
        var embeddingSizes = List.of(64, 256);
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
                            embeddingSizes.stream().flatMap(embeddingSize ->
                                degreesAsProperty.stream().flatMap(degreeAsProperty ->
                                    nodePropertySizes.stream().map(nodePropertySize -> {
                                        var config = ImmutableGraphSageTrainConfig
                                            .builder()
                                            .modelName(modelName)
                                            .username(userName)
                                            .concurrency(concurrency)
                                            .sampleSizes(sampleSizes)
                                            .batchSize(batchSize)
                                            .aggregator(aggregator)
                                            .embeddingSize(embeddingSize)
                                            .degreeAsProperty(degreeAsProperty)
                                            .nodePropertyNames(
                                                IntStream.range(0, nodePropertySize)
                                                    .mapToObj(i -> String.valueOf('a' + i))
                                                    .collect(toList())
                                            )
                                            .build();

                                        return arguments(
                                            config,
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

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

}
