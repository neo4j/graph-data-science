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

import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Node2VecModel {

    private final HugeObjectArray<FloatVector> centerEmbeddings;
    private final HugeObjectArray<FloatVector> contextEmbeddings;
    private final double initialLearningRate;
    private final double minLearningRate;
    private final int iterations;
    private final int embeddingDimension;
    private final int windowSize;
    private final int negativeSamplingRate;
    private final EmbeddingInitializer embeddingInitializer;
    private final Concurrency concurrency;
    private final CompressedRandomWalks walks;
    private final RandomWalkProbabilities randomWalkProbabilities;
    private final ProgressTracker progressTracker;
    private final long randomSeed;

    static final double EPSILON = 1e-10;

    Node2VecModel(
        LongUnaryOperator toOriginalId,
        long nodeCount,
        TrainParameters trainParameters,
        Concurrency concurrency,
        Optional<Long> maybeRandomSeed,
        CompressedRandomWalks walks,
        RandomWalkProbabilities randomWalkProbabilities,
        ProgressTracker progressTracker
    ) {
        this(
            toOriginalId,
            nodeCount,
            trainParameters.initialLearningRate(),
            trainParameters.minLearningRate(),
            trainParameters.iterations(),
            trainParameters.windowSize(),
            trainParameters.negativeSamplingRate(),
            trainParameters.embeddingDimension(),
            trainParameters.embeddingInitializer(),
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
        Concurrency concurrency,
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
        this.randomSeed = maybeRandomSeed.orElseGet(() -> new SplittableRandom().nextLong());

        var random = new Random();
        centerEmbeddings = initializeEmbeddings(toOriginalId, nodeCount, embeddingDimension, random);
        contextEmbeddings = initializeEmbeddings(toOriginalId, nodeCount, embeddingDimension, random);
    }

    Node2VecResult train() {
        progressTracker.beginSubTask();
        var learningRateAlpha = (initialLearningRate - minLearningRate) / iterations;

        var lossPerIteration = new ArrayList<Double>();

        AtomicInteger taskIndex = new AtomicInteger(0);

        for (int iteration = 0; iteration < iterations; iteration++) {
            progressTracker.beginSubTask();
            progressTracker.setVolume(walks.size());

            var learningRate = (float) Math.max(
                minLearningRate,
                initialLearningRate - iteration * learningRateAlpha
            );

            var tasks = createTrainingTasks(learningRate, taskIndex);

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

        return new Node2VecResult(centerEmbeddings, lossPerIteration);
    }

    private HugeObjectArray<FloatVector> initializeEmbeddings(
        LongUnaryOperator toOriginalNodeId,
        long nodeCount,
        int embeddingDimensions,
        Random random
    ) {
        HugeObjectArray<FloatVector> embeddings = HugeObjectArray.newArray(
            FloatVector.class,
            nodeCount
        );
        double bound = switch (embeddingInitializer) {
            case UNIFORM -> 1.0;
            case NORMALIZED -> 0.5 / embeddingDimensions;
        };

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

    List<TrainingTask> createTrainingTasks(float learningRate, AtomicInteger taskIndex) {
        return PartitionUtils.degreePartitionWithBatchSize(
            walks.size(),
            walks::walkLength,
            BitUtil.ceilDiv(randomWalkProbabilities.sampleCount(), concurrency.value()),
            partition -> {
                var taskId = taskIndex.getAndIncrement();
                var taskRandomSeed = randomSeed + taskId;
                var positiveSampleProducer = createPositiveSampleProducer(partition, taskRandomSeed);
                var negativeSampleProducer = createNegativeSampleProducer(taskRandomSeed);
                return new TrainingTask(
                    centerEmbeddings,
                    contextEmbeddings,
                    positiveSampleProducer,
                    negativeSampleProducer,
                    learningRate,
                    negativeSamplingRate,
                    embeddingDimension,
                    progressTracker
                );
            }
        );
    }

    NegativeSampleProducer createNegativeSampleProducer(long randomSeed) {
        return new NegativeSampleProducer(
            randomWalkProbabilities.negativeSamplingDistribution(),
            randomSeed
        );
    }

    PositiveSampleProducer createPositiveSampleProducer(
        DegreePartition partition,
        long randomSeed
    ) {
        return new PositiveSampleProducer(
            walks.iterator(partition.startNode(), partition.nodeCount()),
            randomWalkProbabilities.positiveSamplingProbabilities(),
            windowSize,
            randomSeed
        );
    }

}
