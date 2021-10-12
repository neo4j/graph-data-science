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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.BitUtil;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongCollections;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.Random;

import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.addInPlace;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.scale;

public class Node2VecModel {

    private final NegativeSampleProducer negativeSamples;

    private final HugeObjectArray<FloatVector> centerEmbeddings;
    private final HugeObjectArray<FloatVector> contextEmbeddings;
    private final Node2VecBaseConfig config;
    private final CompressedRandomWalks walks;
    private final RandomWalkProbabilities randomWalkProbabilities;
    private final ProgressTracker progressTracker;
    private final AllocationTracker allocationTracker;

    public static MemoryEstimation memoryEstimation(Node2VecBaseConfig config) {
        var vectorMemoryEstimation = MemoryUsage.sizeOfFloatArray(config.embeddingDimension());

        return MemoryEstimations.builder(Node2Vec.class)
            .perNode(
                "center embeddings",
                (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, vectorMemoryEstimation)
            )
            .perNode(
                "context embeddings",
                (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, vectorMemoryEstimation)
            )
            .build();
    }

    Node2VecModel(
        long nodeCount,
        Node2VecBaseConfig config,
        CompressedRandomWalks walks,
        RandomWalkProbabilities randomWalkProbabilities,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        this.config = config;
        this.walks = walks;
        this.randomWalkProbabilities = randomWalkProbabilities;
        this.progressTracker = progressTracker;
        this.negativeSamples = new NegativeSampleProducer(randomWalkProbabilities.negativeSamplingDistribution());
        this.allocationTracker = allocationTracker;

        centerEmbeddings = initializeEmbeddings(nodeCount, config.embeddingDimension());
        contextEmbeddings = initializeEmbeddings(nodeCount, config.embeddingDimension());
    }

    void train() {
        progressTracker.beginSubTask();
        var learningRateAlpha = (config.initialLearningRate() - config.minLearningRate()) / config.iterations();

        for (int iteration = 0; iteration < config.iterations(); iteration++) {
            progressTracker.beginSubTask();
            progressTracker.setVolume(walks.size());

            var learningRate = (float) Math.max(
                config.minLearningRate(),
                config.initialLearningRate() - iteration * learningRateAlpha
            );

            var tasks = PartitionUtils.degreePartitionWithBatchSize(
                PrimitiveLongCollections.range(0, walks.size() - 1),
                walks::walkLength,
                BitUtil.ceilDiv(randomWalkProbabilities.sampleCount(), config.concurrency()),
                (partition -> {
                    var positiveSampleProducer = new PositiveSampleProducer(
                        walks.iterator(partition.startNode(), partition.nodeCount()),
                        randomWalkProbabilities.positiveSamplingProbabilities(),
                        config.windowSize(),
                        progressTracker
                    );

                    return new TrainingTask(
                        centerEmbeddings,
                        contextEmbeddings,
                        positiveSampleProducer,
                        negativeSamples,
                        learningRate,
                        config.negativeSamplingRate(),
                        config.embeddingDimension()
                    );
                })
            );

            ParallelUtil.runWithConcurrency(config.concurrency(), tasks, Pools.DEFAULT);
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();
    }

    public HugeObjectArray<FloatVector> getEmbeddings() {
        return centerEmbeddings;
    }

    private HugeObjectArray<FloatVector> initializeEmbeddings(long nodeCount, int embeddingDimensions) {
        HugeObjectArray<FloatVector> embeddings = HugeObjectArray.newArray(
            FloatVector.class,
            nodeCount,
            allocationTracker
        );

        var random = new Random();

        for (var i = 0L; i < nodeCount; i++) {
            var data = random
                .doubles(embeddingDimensions, -1, 1)
                .collect(
                    () -> new FloatConsumer(embeddingDimensions),
                    FloatConsumer::add,
                    FloatConsumer::addAll
                ).values;
            embeddings.set(i, new FloatVector(data));
        }
        return embeddings;
    }

    private static final class TrainingTask implements Runnable {
        private final HugeObjectArray<FloatVector> centerEmbeddings;
        private final HugeObjectArray<FloatVector> contextEmbeddings;

        private final PositiveSampleProducer positiveSampleProducer;
        private final NegativeSampleProducer negativeSampleProducer;
        private final FloatVector centerGradientBuffer;
        private final FloatVector contextGradientBuffer;
        private final int negativeSamplingRate;
        private final float learningRate;

        private TrainingTask(
            HugeObjectArray<FloatVector> centerEmbeddings,
            HugeObjectArray<FloatVector> contextEmbeddings,
            PositiveSampleProducer positiveSampleProducer,
            NegativeSampleProducer negativeSampleProducer,
            float learningRate,
            int negativeSamplingRate,
            int embeddingDimensions
        ) {
            this.centerEmbeddings = centerEmbeddings;
            this.contextEmbeddings = contextEmbeddings;
            this.positiveSampleProducer = positiveSampleProducer;
            this.negativeSampleProducer = negativeSampleProducer;
            this.learningRate = learningRate;
            this.negativeSamplingRate = negativeSamplingRate;

            this.centerGradientBuffer = new FloatVector(embeddingDimensions);
            this.contextGradientBuffer = new FloatVector(embeddingDimensions);
        }

        @Override
        public void run() {
            var buffer = new long[2];
            while (positiveSampleProducer.next(buffer)) {
                trainSample(buffer[0], buffer[1], true);

                for (var i = 0; i < negativeSamplingRate; i++) {
                    trainSample(buffer[0], negativeSampleProducer.next(), false);
                }
            }
        }

        private void trainSample(long center, long context, boolean positive) {
            var centerEmbedding = centerEmbeddings.get(center);
            var contextEmbedding = contextEmbeddings.get(context);

            float affinity = positive
                ? centerEmbedding.innerProduct(contextEmbedding)
                : -centerEmbedding.innerProduct(contextEmbedding);

            float scalar = (float) (positive
                ? 1 / (Math.exp(affinity) + 1)
                : -1 / (Math.exp(affinity) + 1));

            scale(contextEmbedding.data(), scalar * learningRate, centerGradientBuffer.data());
            scale(centerEmbedding.data(), scalar * learningRate, contextGradientBuffer.data());

            addInPlace(centerEmbedding.data(), centerGradientBuffer.data());
            addInPlace(contextEmbedding.data(), contextGradientBuffer.data());
        }
    }

    static class FloatConsumer {
        float[] values;
        int index;

        FloatConsumer(int length) {
            this.values = new float[length];
        }

        void add(double value) {
            values[index++] = (float) value;
        }

        void addAll(FloatConsumer other) {
            System.arraycopy(other.values, 0, values, index, other.index);
            index += other.index;
        }
    }
}
