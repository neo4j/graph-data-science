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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongCollections;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.LongUnaryOperator;
import java.util.stream.IntStream;

import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.addInPlace;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.scale;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Node2VecModel {

    private final NegativeSampleProducer negativeSamples;

    private final HugeObjectArray<FloatVector> centerEmbeddings;
    private final HugeObjectArray<FloatVector> contextEmbeddings;
    private final Node2VecBaseConfig config;
    private final CompressedRandomWalks walks;
    private final RandomWalkProbabilities randomWalkProbabilities;
    private final ProgressTracker progressTracker;
    private final long randomSeed;

    public static MemoryEstimation memoryEstimation(Node2VecBaseConfig config) {
        var vectorMemoryEstimation = MemoryUsage.sizeOfFloatArray(config.embeddingDimension());

        return MemoryEstimations.builder(Node2Vec.class.getSimpleName())
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
        LongUnaryOperator toOriginalId,
        long nodeCount,
        Node2VecBaseConfig config,
        CompressedRandomWalks walks,
        RandomWalkProbabilities randomWalkProbabilities,
        ProgressTracker progressTracker
    ) {
        this.config = config;
        this.walks = walks;
        this.randomWalkProbabilities = randomWalkProbabilities;
        this.progressTracker = progressTracker;
        this.negativeSamples = new NegativeSampleProducer(randomWalkProbabilities.negativeSamplingDistribution());
        this.randomSeed = config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong());

        var random = new Random();
        centerEmbeddings = initializeEmbeddings(toOriginalId, nodeCount, config.embeddingDimension(), random);
        contextEmbeddings = initializeEmbeddings(toOriginalId, nodeCount, config.embeddingDimension(), random);
    }

    Result train() {
        progressTracker.beginSubTask();
        var learningRateAlpha = (config.initialLearningRate() - config.minLearningRate()) / config.iterations();

        var lossPerIteration = new ArrayList<Double>();

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
                partition -> {
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
                }
            );

            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(tasks)
                .run();

            double loss = tasks.stream().mapToDouble(TrainingTask::lossSum).sum();
            progressTracker.logInfo(formatWithLocale("Loss %.4f", loss));
            lossPerIteration.add(loss);

            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();

        return ImmutableResult.of(centerEmbeddings, lossPerIteration);
    }

    private HugeObjectArray<FloatVector> initializeEmbeddings(LongUnaryOperator toOriginalNodeId, long nodeCount, int embeddingDimensions, Random random) {
        HugeObjectArray<FloatVector> embeddings = HugeObjectArray.newArray(
            FloatVector.class,
            nodeCount
        );
        double bound;
        switch (config.initializationBound()) {
            case "default":
                bound = 1.0;
                break;
            case "xavier":
                bound = Math.sqrt(6.0 / embeddingDimensions);
                break;
            case "gensim":
                bound = 0.5 / embeddingDimensions;
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + config.initializationBound());
        }

        for (var i = 0L; i < nodeCount; i++) {
            random.setSeed(toOriginalNodeId.applyAsLong(i) + randomSeed);
            var dataStream = config.gaussianInitialization()
                ? IntStream.range(0, embeddingDimensions).mapToDouble(ignore -> bound * random.nextGaussian())
                : random.doubles(embeddingDimensions, -bound, bound);
            var data = dataStream
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

        private double lossSum;

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

            // this corresponds to a stochastic optimizer as the embeddings are updated after each sample
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

            // L_pos = -log sigmoid(center * context)  ; gradient: -sigmoid (-center * context)
            // L_neg = -log sigmoid(-center * context) ; gradient: sigmoid (center * context)
            float affinity = centerEmbedding.innerProduct(contextEmbedding);

            float positiveSigmoid = (float) Sigmoid.sigmoid(affinity);
            float negativeSigmoid = 1 - positiveSigmoid;


            lossSum -= positive ? Math.log(positiveSigmoid) : Math.log(negativeSigmoid);

            float gradient = positive ? -negativeSigmoid : positiveSigmoid;
            // we are doing gradient descent, so we go in the negative direction of the gradient here
            float scaledGradient = -gradient * learningRate;

            scale(contextEmbedding.data(), scaledGradient, centerGradientBuffer.data());
            scale(centerEmbedding.data(), scaledGradient, contextGradientBuffer.data());

            addInPlace(centerEmbedding.data(), centerGradientBuffer.data());
            addInPlace(contextEmbedding.data(), contextGradientBuffer.data());
        }

        double lossSum() {
            return lossSum;
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

    @ValueClass
    public interface Result {
        HugeObjectArray<FloatVector> embeddings();

        List<Double> lossPerIteration();
    }
}
