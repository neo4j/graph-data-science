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
package org.neo4j.gds.embeddings.fastrp;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.features.FeatureConsumer;
import org.neo4j.gds.ml.features.FeatureExtraction;
import org.neo4j.gds.ml.features.FeatureExtractor;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class FastRP extends Algorithm<FastRP, FastRP.FastRPResult> {

    private static final int MIN_BATCH_SIZE = 1;
    private static final int SPARSITY = 3;
    private static final double ENTRY_PROBABILITY = 1.0 / (2 * SPARSITY);

    private final Graph graph;
    private final int concurrency;
    private final float normalizationStrength;
    private final List<FeatureExtractor> featureExtractors;
    private final int inputDimension;
    private final float[][] propertyVectors;
    private final HugeObjectArray<float[]> embeddings;
    private final HugeObjectArray<float[]> embeddingA;
    private final HugeObjectArray<float[]> embeddingB;
    private final EmbeddingCombiner embeddingCombiner;
    private final long randomSeed;

    private final int embeddingDimension;
    private final int baseEmbeddingDimension;
    private final List<Number> iterationWeights;

    public static MemoryEstimation memoryEstimation(FastRPBaseConfig config) {
        return MemoryEstimations
            .builder(FastRP.class)
            .fixed(
                "propertyVectors",
                MemoryUsage.sizeOfFloatArray(config.featureProperties().size() * config.propertyDimension())
            )
            .add("embeddings", HugeObjectArray.memoryEstimation(MemoryUsage.sizeOfFloatArray(config.embeddingDimension())))
            .add("embeddingA", HugeObjectArray.memoryEstimation(MemoryUsage.sizeOfFloatArray(config.embeddingDimension())))
            .add("embeddingB", HugeObjectArray.memoryEstimation(MemoryUsage.sizeOfFloatArray(config.embeddingDimension())))
            .build();
    }

    public FastRP(
        Graph graph,
        FastRPBaseConfig config,
        List<FeatureExtractor> featureExtractors,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        this(graph, config, featureExtractors, progressLogger, tracker, config.randomSeed());
    }

    public FastRP(
        Graph graph,
        FastRPBaseConfig config,
        List<FeatureExtractor> featureExtractors,
        ProgressLogger progressLogger,
        AllocationTracker tracker,
        Optional<Long> randomSeed
    ) {
        this.graph = graph;
        this.featureExtractors = featureExtractors;
        this.inputDimension = FeatureExtraction.featureCount(featureExtractors);
        this.randomSeed = improveSeed(randomSeed.orElseGet(System::nanoTime));
        this.progressLogger = progressLogger;

        this.propertyVectors = new float[inputDimension][config.propertyDimension()];
        this.embeddings = HugeObjectArray.newArray(float[].class, graph.nodeCount(), tracker);
        this.embeddingA = HugeObjectArray.newArray(float[].class, graph.nodeCount(), tracker);
        this.embeddingB = HugeObjectArray.newArray(float[].class, graph.nodeCount(), tracker);
        // Each of the above arrays will contain a float array of size `embeddingDimension` for each node.
        tracker.add(3 * graph.nodeCount() * MemoryUsage.sizeOfFloatArray(config.embeddingDimension()));

        this.embeddingDimension = config.embeddingDimension();
        this.baseEmbeddingDimension = config.embeddingDimension() - config.propertyDimension();
        this.iterationWeights = config.iterationWeights();
        this.normalizationStrength = config.normalizationStrength();
        this.concurrency = config.concurrency();
        this.embeddingCombiner = graph.hasRelationshipProperty()
            ? this::addArrayValuesWeighted
            : (lhs, rhs, ignoreWeight) -> addArrayValues(lhs, rhs);
        this.embeddings.setAll((i) -> new float[embeddingDimension]);
    }

    @Override
    public FastRPResult compute() {
        progressLogger.logStart();
        initPropertyVectors();
        initRandomVectors();
        propagateEmbeddings();
        progressLogger.logFinish();
        return new FastRPResult(embeddings);
    }

    @Override
    public FastRP me() {
        return this;
    }

    @Override
    public void release() {
        this.embeddingA.release();
        this.embeddingB.release();
    }

    void initPropertyVectors() {
        int propertyDimension = embeddingDimension - baseEmbeddingDimension;
        float entryValue = (float) Math.sqrt(SPARSITY) / (float) Math.sqrt(propertyDimension);
        var random = new HighQualityRandom(randomSeed);
        for (int i = 0; i < inputDimension; i++) {
            this.propertyVectors[i] = new float[propertyDimension];
            for (int d = 0; d < propertyDimension; d++) {
                this.propertyVectors[i][d] = computeRandomEntry(random, entryValue);
            }
        }
    }

    void initRandomVectors() {
        progressLogger.logMessage("Initialising Random Vectors :: Start");

        long batchSize = ParallelUtil.adjustedBatchSize(graph.nodeCount(), concurrency, MIN_BATCH_SIZE);
        float sqrtEmbeddingDimension = (float) Math.sqrt(baseEmbeddingDimension);
        List<Runnable> tasks = PartitionUtils.rangePartition(concurrency, graph.nodeCount(), batchSize)
            .stream()
            .map(partition -> new InitRandomVectorTask(
                partition,
                sqrtEmbeddingDimension
            ))
            .collect(Collectors.toList());
        ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

        progressLogger.logMessage("Initialising Random Vectors :: Finished");
    }

    void propagateEmbeddings() {
        long batchSize = ParallelUtil.adjustedBatchSize(graph.nodeCount(), concurrency, MIN_BATCH_SIZE);
        for (int i = 0; i < iterationWeights.size(); i++) {
            progressLogger.reset(graph.relationshipCount());
            progressLogger.logMessage(formatWithLocale("Iteration %s :: Start", i + 1));

            var localCurrent = i % 2 == 0 ? embeddingA : embeddingB;
            var localPrevious = i % 2 == 0 ? embeddingB : embeddingA;
            double iterationWeight = iterationWeights.get(i).doubleValue();

            List<Runnable> tasks = PartitionUtils.rangePartition(concurrency, graph.nodeCount(), batchSize)
                .stream()
                .map(partition -> new PropagateEmbeddingsTask(
                        partition,
                        localCurrent,
                        localPrevious,
                        iterationWeight
                    )
                )
                .collect(Collectors.toList());
            ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

            progressLogger.logMessage(formatWithLocale("Iteration %s :: Finished", i + 1));
        }
    }

    @TestOnly
    HugeObjectArray<float[]> currentEmbedding(int iteration) {
        return iteration % 2 == 0
            ? this.embeddingA
            : this.embeddingB;
    }

    @TestOnly
    HugeObjectArray<float[]> embeddings() {
        return embeddings;
    }

    private void addArrayValues(float[] lhs, float[] rhs) {
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] += rhs[i];
        }
    }

    private void addArrayValuesWeighted(float[] lhs, float[] rhs, double weight) {
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] = (float) Math.fma(rhs[i], weight, lhs[i]);
        }
    }

    private static void multiplyArrayValues(float[] lhs, double scalar) {
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] *= scalar;
        }
    }

    static void l2Normalize(float[] array) {
        double sum = 0.0f;
        for (double value : array) {
            sum += value * value;
        }
        double sqrtSum = sum == 0 ? 1 : Math.sqrt(sum);
        double scaling = 1 / sqrtSum;
        for (int i = 0; i < array.length; i++) {
            array[i] *= scaling;
        }
    }

    private static void updateEmbeddings(double weight, float[] embedding, float[] newEmbedding) {
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] += weight * newEmbedding[i];
        }
    }

    private static float computeRandomEntry(Random random, float entryValue) {
        double randomValue = random.nextDouble();

        if (randomValue < ENTRY_PROBABILITY) {
            return entryValue;
        } else if (randomValue < ENTRY_PROBABILITY * 2.0) {
            return -entryValue;
        } else {
            return 0.0f;
        }
    }

    private static class HighQualityRandom extends Random {
        private long u;
        private long v;
        private long w;

        public HighQualityRandom(long seed) {
            reseed(seed);
        }

        public void reseed(long seed) {
            v = 4101842887655102017L;
            w = 1;
            u = seed ^ v;
            nextLong();
            v = u;
            nextLong();
            w = v;
            nextLong();
        }

        @Override
        public long nextLong() {
            u = u * 2862933555777941757L + 7046029254386353087L;
            v ^= v >>> 17;
            v ^= v << 31;
            v ^= v >>> 8;
            w = 4294957665L * w + (w >>> 32);
            long x = u ^ (u << 21);
            x ^= x >>> 35;
            x ^= x << 4;
            return (x + v) ^ w;
        }

        protected int next(int bits) {
            return (int) (nextLong() >>> (64-bits));
        }
    }

    private long improveSeed(long randomSeed) {
        return new HighQualityRandom(randomSeed).nextLong();
    }

    private interface EmbeddingCombiner {
        void combine(float[] into, float[] add, double weight);
    }

    private final class InitRandomVectorTask implements Runnable {

        final float sqrtSparsity = (float) Math.sqrt(SPARSITY);

        private final Partition partition;
        private final float sqrtEmbeddingDimension;

        private InitRandomVectorTask(
            Partition partition,
            float sqrtEmbeddingDimension
        ) {
            this.partition = partition;
            this.sqrtEmbeddingDimension = sqrtEmbeddingDimension;
        }

        @Override
        public void run() {
            // this value currently doesnt matter because of reseeding below
            var random = new HighQualityRandom(randomSeed);
            for (long nodeId = partition.startNode(); nodeId < partition.startNode() + partition.nodeCount(); nodeId++) {
                int degree = graph.degree(nodeId);
                float scaling = degree == 0
                    ? 1.0f
                    : (float) Math.pow(degree, normalizationStrength);

                float entryValue = scaling * sqrtSparsity / sqrtEmbeddingDimension;
                random.reseed(randomSeed ^ nodeId);
                float[] randomVector = computeRandomVector(nodeId, random, entryValue);
                embeddingB.set(nodeId, randomVector);
                embeddingA.set(nodeId, new float[embeddingDimension]);
            }
            progressLogger.logProgress(partition.nodeCount());
        }

        private float[] computeRandomVector(long nodeId, Random random, float entryValue) {
            float[] randomVector = new float[embeddingDimension];
            for (int i = 0; i < baseEmbeddingDimension; i++) {
                randomVector[i] = computeRandomEntry(random, entryValue);
            }

            float[] features = features(nodeId);

            for (int j = 0; j < features.length; j++) {
                double featureValue = features[j];
                if (featureValue != 0.0D) {
                    for (int i = baseEmbeddingDimension; i < embeddingDimension; i++) {
                        randomVector[i] += featureValue * propertyVectors[j][i - baseEmbeddingDimension];
                    }
                }
            }
            return randomVector;
        }

        float[] features(long nodeId) {
            var features = new float[inputDimension];
            FeatureConsumer featureConsumer = new FeatureConsumer() {
                @Override
                public void acceptScalar(long ignored, int offset, double value) {
                    features[offset] = (float)value;
                }

                @Override
                public void acceptArray(long ignored, int offset, double[] values) {
                    for (int i = 0; i < values.length; i++) {
                        features[offset + i] = (float)values[i];
                    }
                }
            };
            FeatureExtraction.extract(nodeId, -1, featureExtractors, featureConsumer);
            return features;
        }
    }

    private final class PropagateEmbeddingsTask implements Runnable {

        private final Partition partition;
        private final HugeObjectArray<float[]> localCurrent;
        private final HugeObjectArray<float[]> localPrevious;
        private final double iterationWeight;
        private final Graph concurrentGraph;

        private PropagateEmbeddingsTask(
            Partition partition,
            HugeObjectArray<float[]> localCurrent,
            HugeObjectArray<float[]> localPrevious,
            double iterationWeight
        ) {
            this.partition = partition;
            this.localCurrent = localCurrent;
            this.localPrevious = localPrevious;
            this.iterationWeight = iterationWeight;
            this.concurrentGraph = graph.concurrentCopy();
        }

        @Override
        public void run() {
            long degrees = 0;
            for (long nodeId = partition.startNode(); nodeId < partition.startNode() + partition.nodeCount(); nodeId++) {
                float[] embedding = embeddings.get(nodeId);
                float[] currentEmbedding = localCurrent.get(nodeId);
                Arrays.fill(currentEmbedding, 0.0f);

                // Collect and combine the neighbour embeddings
                concurrentGraph.forEachRelationship(nodeId, 1.0, (source, target, weight) -> {
                    embeddingCombiner.combine(currentEmbedding, localPrevious.get(target), weight);
                    return true;
                });

                // Normalize neighbour embeddings
                var degree = graph.degree(nodeId);
                int adjustedDegree = degree == 0 ? 1 : degree;
                double degreeScale = 1.0f / adjustedDegree;
                multiplyArrayValues(currentEmbedding, degreeScale);
                l2Normalize(currentEmbedding);

                // Update the result embedding
                updateEmbeddings(iterationWeight, embedding, currentEmbedding);
                degrees += degree;
            }
            progressLogger.logProgress(degrees);
        }
    }

    public static class FastRPResult {
        private final HugeObjectArray<float[]> embeddings;

        public FastRPResult(HugeObjectArray<float[]> embeddings) {
            this.embeddings = embeddings;
        }

        public HugeObjectArray<float[]> embeddings() {
            return embeddings;
        }
    }
}
