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

import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.ArrayList;
import java.util.Random;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Node2VecModel {

    private final NegativeSampleProducer negativeSamples;

    private final HugeObjectArray<Vector> centerEmbeddings;
    private final HugeObjectArray<Vector> contextEmbeddings;
    private final Node2VecBaseConfig config;
    private final HugeObjectArray<long[]> walks;
    private final ProbabilityComputer probabilityComputer;
    private final ProgressLogger progressLogger;
    private final AllocationTracker tracker;
    private final long batchSize;

    public static MemoryEstimation memoryEstimation(Node2VecBaseConfig config) {
        var vectorMemoryEstimation = MemoryUsage.sizeOfFloatArray(config.embeddingDimension());

        return MemoryEstimations.builder(Node2Vec.class)
            .perNode("center embeddings", (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, vectorMemoryEstimation))
            .perNode("context embeddings", (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, vectorMemoryEstimation))
            .build();
    }

    Node2VecModel(
        long nodeCount,
        Node2VecBaseConfig config,
        HugeObjectArray<long[]> walks,
        ProbabilityComputer probabilityComputer,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this.config = config;
        this.walks = walks;
        this.probabilityComputer = probabilityComputer;
        this.progressLogger = progressLogger;
        this.negativeSamples = new NegativeSampleProducer(probabilityComputer.getContextNodeDistribution());
        this.tracker = tracker;

        // TODO research how the weights are initialized
        centerEmbeddings = initializeEmbeddings(nodeCount, config.embeddingDimension());
        contextEmbeddings = initializeEmbeddings(nodeCount, config.embeddingDimension());

        this.batchSize = ParallelUtil.adjustedBatchSize(
            walks.size(),
            config.concurrency(),
            1000
        );
    }

    void train() {
        progressLogger.logMessage(":: Training :: Start");
        for (int iteration = 0; iteration < config.iterations(); iteration++) {
            progressLogger.reset(walks.size());
            progressLogger.logMessage(formatWithLocale(":: Iteration %d :: Start", iteration + 1));
            var tasks = new ArrayList<TrainingTask>();
            for (long sampleIndex = 0; sampleIndex < walks.size(); sampleIndex += batchSize) {
                tasks.add(new TrainingTask(sampleIndex, Math.min(walks.size(), sampleIndex + batchSize) - 1));
            }
            ParallelUtil.runWithConcurrency(config.concurrency(), tasks, Pools.DEFAULT);
            progressLogger.logMessage(formatWithLocale(":: Iteration %d :: Finished", iteration + 1));
        }
        progressLogger.logMessage(":: Training :: Finished");
    }

    public HugeObjectArray<Vector> getEmbeddings() {
        return centerEmbeddings;
    }

    private HugeObjectArray<Vector> initializeEmbeddings(long nodeCount, int embeddingDimensions) {
        HugeObjectArray<Vector> embeddings = HugeObjectArray.newArray(
            Vector.class,
            nodeCount,
            tracker
        );
        for (var i = 0L; i < nodeCount; i++) {
            var data = new Random()
                .doubles(embeddingDimensions, -1, 1)
                .collect(() -> new FloatConsumer(embeddingDimensions), FloatConsumer::add, FloatConsumer::addAll).values;
            embeddings.set(i, new Vector(data));
        }
        return embeddings;
    }

    private class TrainingTask implements Runnable {
        private final PositiveSampleProducer positiveSamples;
        private final Vector centerGradientBuffer;
        private final Vector contextGradientBuffer;
        private final float initialLearningRate;
        private final float learningRateModifier;
        private final long startIndex;

        private float learningRate;

        TrainingTask(long startIndex, long endIndex) {
            this.startIndex = startIndex;
            this.positiveSamples = new PositiveSampleProducer(
                walks,
                probabilityComputer.getCenterNodeProbabilities(),
                startIndex,
                endIndex,
                config.windowSize(),
                progressLogger
            );
            this.centerGradientBuffer = new Vector(config.embeddingDimension());
            this.contextGradientBuffer = new Vector(config.embeddingDimension());

            this.initialLearningRate = (float) config.initialLearningRate();
            this.learningRateModifier = (float) ((initialLearningRate - config.minLearningRate()) / (endIndex - startIndex));
            this.learningRate = initialLearningRate;
        }

        @Override
        public void run() {
            var buffer = new long[2];
            while (positiveSamples.hasNext()) {
                positiveSamples.next(buffer);
                trainSample(buffer[0], buffer[1], true);

                for (var i = 0; i < config.negativeSamplingRate(); i++) {
                    trainSample(buffer[0], negativeSamples.nextSample(), false);
                }

                learningRate = initialLearningRate - (learningRateModifier * (positiveSamples.currentWalkIndex() - startIndex));
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

            centerGradientBuffer.scalarMultiply(contextEmbedding, scalar * learningRate);
            contextGradientBuffer.scalarMultiply(centerEmbedding, scalar * learningRate);

            centerEmbedding.addMutable(centerGradientBuffer);
            contextEmbedding.addMutable(contextGradientBuffer);
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
