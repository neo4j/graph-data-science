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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.addInPlace;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.scale;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Node2VecModel {

    private final NegativeSampleProducer negativeSamples;

    private final HugeObjectArray<FloatVector> centerEmbeddings;
    private final HugeObjectArray<FloatVector> contextEmbeddings;
    private final double initialLearningRate;
    private final double minLearningRate;
    private final int iterations;
    private final int embeddingDimension;
    private final int windowSize;
    private final int negativeSamplingRate;
    private final EmbeddingInitializer embeddingInitializer;
    private final int concurrency;
    private final CompressedRandomWalks walks;
    private final RandomWalkProbabilities randomWalkProbabilities;
    private final ProgressTracker progressTracker;
    private final long randomSeed;

    private static final double EPSILON = 1e-10;

    Node2VecModel(
        LongUnaryOperator toOriginalId,
        long nodeCount,
        TrainParameters trainParameters,
        int concurrency,
        Optional<Long> maybeRandomSeed,
        CompressedRandomWalks walks,
        RandomWalkProbabilities randomWalkProbabilities,
        ProgressTracker progressTracker
    ) {
        this(
            toOriginalId,
            nodeCount,
            trainParameters.initialLearningRate,
            trainParameters.minLearningRate,
            trainParameters.iterations,
            trainParameters.windowSize,
            trainParameters.negativeSamplingRate,
            trainParameters.embeddingDimension,
            trainParameters.embeddingInitializer,
            concurrency,
            maybeRandomSeed,
            walks,
            randomWalkProbabilities,
            progressTracker
        );
    }

    Node2VecModel(
        LongUnaryOperator toOriginalId,
        long nodeCount,
        double initialLearningRate,
        double minLearningRate,
        int iterations,
        int windowSize,
        int negativeSamplingRate,
        int embeddingDimension,
        EmbeddingInitializer embeddingInitializer,
        int concurrency,
        Optional<Long> maybeRandomSeed,
        CompressedRandomWalks walks,
        RandomWalkProbabilities randomWalkProbabilities,
        ProgressTracker progressTracker
    ) {
        this.initialLearningRate = initialLearningRate;
        this.minLearningRate = minLearningRate;
        this.iterations = iterations;
        this.embeddingDimension = embeddingDimension;
        this.windowSize = windowSize;
        this.negativeSamplingRate = negativeSamplingRate;
        this.embeddingInitializer = embeddingInitializer;
        this.concurrency = concurrency;
        this.walks = walks;
        this.randomWalkProbabilities = randomWalkProbabilities;
        this.progressTracker = progressTracker;
        this.negativeSamples = new NegativeSampleProducer(randomWalkProbabilities.negativeSamplingDistribution());
        this.randomSeed = maybeRandomSeed.orElseGet(() -> new SplittableRandom().nextLong());

        var random = new Random();
        centerEmbeddings = initializeEmbeddings(toOriginalId, nodeCount, embeddingDimension, random);
        contextEmbeddings = initializeEmbeddings(toOriginalId, nodeCount, embeddingDimension, random);
    }

    Result train() {
        progressTracker.beginSubTask();
        var learningRateAlpha = (initialLearningRate - minLearningRate) / iterations;

        var lossPerIteration = new ArrayList<Double>();

        for (int iteration = 0; iteration < iterations; iteration++) {
            progressTracker.beginSubTask();
            progressTracker.setVolume(walks.size());

            var learningRate = (float) Math.max(
                minLearningRate,
                initialLearningRate - iteration * learningRateAlpha
            );

            var tasks = PartitionUtils.degreePartitionWithBatchSize(
                walks.size(),
                walks::walkLength,
                BitUtil.ceilDiv(randomWalkProbabilities.sampleCount(), concurrency),
                partition -> {
                    var positiveSampleProducer = new PositiveSampleProducer(
                        walks.iterator(partition.startNode(), partition.nodeCount()),
                        randomWalkProbabilities.positiveSamplingProbabilities(),
                        windowSize
                    );

                    return new TrainingTask(
                        centerEmbeddings,
                        contextEmbeddings,
                        positiveSampleProducer,
                        negativeSamples,
                        learningRate,
                        negativeSamplingRate,
                        embeddingDimension,
                        progressTracker
                    );
                }
            );

            RunWithConcurrency.builder()
                .concurrency(concurrency)
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
        switch (embeddingInitializer) {
            case UNIFORM:
                bound = 1.0;
                break;
            case NORMALIZED:
                bound = 0.5 / embeddingDimensions;
                break;
            default:
                throw new IllegalStateException("Missing implementation for: " + embeddingInitializer);
        }

        for (var i = 0L; i < nodeCount; i++) {
            random.setSeed(toOriginalNodeId.applyAsLong(i) + randomSeed);
            var data = random.doubles(embeddingDimensions, -bound, bound)
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

        private final ProgressTracker progressTracker;

        private double lossSum;

        private TrainingTask(
            HugeObjectArray<FloatVector> centerEmbeddings,
            HugeObjectArray<FloatVector> contextEmbeddings,
            PositiveSampleProducer positiveSampleProducer,
            NegativeSampleProducer negativeSampleProducer,
            float learningRate,
            int negativeSamplingRate,
            int embeddingDimensions,
            ProgressTracker progressTracker
        ) {
            this.centerEmbeddings = centerEmbeddings;
            this.contextEmbeddings = contextEmbeddings;
            this.positiveSampleProducer = positiveSampleProducer;
            this.negativeSampleProducer = negativeSampleProducer;
            this.learningRate = learningRate;
            this.negativeSamplingRate = negativeSamplingRate;

            this.centerGradientBuffer = new FloatVector(embeddingDimensions);
            this.contextGradientBuffer = new FloatVector(embeddingDimensions);
            this.progressTracker = progressTracker;
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
                progressTracker.logProgress();
            }
        }

        private void trainSample(long center, long context, boolean positive) {
            var centerEmbedding = centerEmbeddings.get(center);
            var contextEmbedding = contextEmbeddings.get(context);

            // L_pos = -log sigmoid(center * context)  ; gradient: -sigmoid (-center * context)
            // L_neg = -log sigmoid(-center * context) ; gradient: sigmoid (center * context)
            float affinity = centerEmbedding.innerProduct(contextEmbedding);

            //When |affinity| > 40, positiveSigmoid = 1. Double precision is not enough.
            //Make sure negativeSigmoid can never be 0 to avoid infinity loss.
            double positiveSigmoid = Sigmoid.sigmoid(affinity);
            double negativeSigmoid = 1 - positiveSigmoid;

            lossSum -= positive ? Math.log(positiveSigmoid+EPSILON) : Math.log(negativeSigmoid+EPSILON);

            float gradient = positive ? (float) -negativeSigmoid : (float) positiveSigmoid;
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
