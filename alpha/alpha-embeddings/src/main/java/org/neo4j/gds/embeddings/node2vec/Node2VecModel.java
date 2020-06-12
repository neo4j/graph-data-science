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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
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
    private final long batchSize;

    Node2VecModel(
        long nodeCount,
        Node2VecBaseConfig config,
        HugeObjectArray<long[]> walks,
        ProbabilityComputer probabilityComputer,
        ProgressLogger progressLogger
    ) {
        this.config = config;
        this.walks = walks;
        this.probabilityComputer = probabilityComputer;
        this.progressLogger = progressLogger;
        this.negativeSamples = new NegativeSampleProducer(probabilityComputer.getContextNodeDistribution());

        // TODO research how the weights are initialized
        centerEmbeddings = initializeEmbeddings(nodeCount, config.dimensions());
        contextEmbeddings = initializeEmbeddings(nodeCount, config.dimensions());

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
        HugeObjectArray<Vector> embeddings = HugeObjectArray.newArray(Vector.class, nodeCount, AllocationTracker.EMPTY);
        for (var i = 0L; i < nodeCount; i++) {
            double[] data = new Random()
                .doubles(embeddingDimensions, -1, 1)
                .toArray();
            embeddings.set(i, new Vector(data));
        }
        return embeddings;
    }

    private class TrainingTask implements Runnable {
        private final PositiveSampleProducer positiveSamples;
        private final Vector centerGradientBuffer;
        private final Vector contextGradientBuffer;
        private final double learningRate;
        private final double learningRateModifier;
        private final long startIndex;

        private double currentLearningRate;

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
            this.centerGradientBuffer = new Vector(config.dimensions());
            this.contextGradientBuffer = new Vector(config.dimensions());

            this.learningRate = config.learningRate();
            this.learningRateModifier = (learningRate - config.minLearningRate()) / (endIndex - startIndex);
            this.currentLearningRate = learningRate;
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

                currentLearningRate = learningRate - (learningRateModifier * (positiveSamples.currentWalkIndex() - startIndex));
            }
        }

        private void trainSample(long center, long context, boolean positive) {
            var centerEmbedding = centerEmbeddings.get(center);
            var contextEmbedding = contextEmbeddings.get(context);

            double affinity = positive
                ? centerEmbedding.innerProduct(contextEmbedding)
                : -centerEmbedding.innerProduct(contextEmbedding);

            double scalar = positive
                ? 1 / (Math.exp(affinity) + 1)
                : -1 / (Math.exp(affinity) + 1);

            centerGradientBuffer.scalarMultiply(contextEmbedding, scalar * currentLearningRate);
            contextGradientBuffer.scalarMultiply(centerEmbedding, scalar * currentLearningRate);

            centerEmbedding.addMutable(centerGradientBuffer);
            contextEmbedding.addMutable(contextGradientBuffer);
        }
    }
}
