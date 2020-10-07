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

import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.Layer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

class GraphSageTrainAlgorithmFactoryTest {

    @ParameterizedTest
    @MethodSource("parameters")
    void memoryEstimation(
        long nodeCount,
        LongUnaryOperator hugeObjectArraySize,
        int concurrency,
        int numberOfProperties,
        boolean degreeAsProperty
    ) {
        // times concurrency: *
        // 3 times batch size = 3bs

        // embeddings: start
        // features: HugeOA[nodeCount * double[featureSize]]

        // layer representation = parent = local features: double[3bs * featureSize]

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


        int batchSize = 100;
        var userName = "userName";
        var modelName = "modelName";
        int featureSize = numberOfProperties + (degreeAsProperty ? 1 : 0);

        var trainConfig = ImmutableGraphSageTrainConfig
            .builder()
            .modelName(modelName)
            .username(userName)
            .degreeAsProperty(degreeAsProperty)
            .embeddingSize(numberOfProperties)
            .nodePropertyNames(
                IntStream.range(0, numberOfProperties)
                    .mapToObj(i -> String.valueOf('a' + i))
                    .collect(Collectors.toList())
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
            .modelName(modelName)
            .username(userName)
            .batchSize(batchSize)
            .build();

        MemoryEstimation estimation = new GraphSageAlgorithmFactory<GraphSageStreamConfig>()
            .memoryEstimation(streamConfig);

        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();

        MemoryTree estimate = estimation.estimate(dimensions, concurrency);
        MemoryRange actual = estimate.memoryUsage();

        long doubleArray = sizeOfDoubleArray(featureSize);
        long hugeObjectArray = hugeObjectArraySize.applyAsLong(doubleArray);
        long perThreadArray = sizeOfDoubleArray(batchSize * featureSize);
        long instanceSize = 40;
        long expected = instanceSize + 2 * hugeObjectArray + concurrency * perThreadArray;

        assertEquals(expected, actual.min);
        assertEquals(expected, actual.max);
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

        var concurrencies = List.of(1, 2, 4, 20, 42);
        var embeddingSizes = List.of(1, 9, 42);
        var degreesAsProperty = List.of(true, false);

        return nodeCounts.flatMap(nodeCountPair -> {
            var nodeCount = nodeCountPair.getOne();
            var hugeObjectArraySize = nodeCountPair.getTwo();

            return concurrencies.stream().flatMap(concurrency ->
                embeddingSizes.stream().flatMap(embeddingSize ->
                    degreesAsProperty.stream().map(degreeAsProperty ->
                        arguments(
                            nodeCount,
                            hugeObjectArraySize,
                            concurrency,
                            embeddingSize,
                            degreeAsProperty
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
