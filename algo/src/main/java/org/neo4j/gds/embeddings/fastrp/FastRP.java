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

import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.features.FeatureConsumer;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.addInPlace;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.addWeightedInPlace;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.l2Norm;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.l2Normalize;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.scale;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class FastRP extends Algorithm<FastRP.FastRPResult> {

    private static final int SPARSITY = 3;
    private static final double ENTRY_PROBABILITY = 1.0 / (2 * SPARSITY);
    private static final float EPSILON = 10f / Float.MAX_VALUE;

    private final Graph graph;
    private final int concurrency;
    private final float normalizationStrength;
    private final List<FeatureExtractor> featureExtractors;
    private final Optional<String> relationshipWeightProperty;
    private final double relationshipWeightFallback;
    private final int inputDimension;
    private final float[][] propertyVectors;
    private final HugeObjectArray<float[]> embeddings;
    private final HugeObjectArray<float[]> embeddingA;
    private final HugeObjectArray<float[]> embeddingB;
    private final EmbeddingCombiner embeddingCombiner;
    private final long randomSeed;

    private final int embeddingDimension;
    private final int baseEmbeddingDimension;
    private final Number nodeSelfInfluence;
    private final List<Number> iterationWeights;
    private final int minBatchSize;
    private List<DegreePartition> partitions;

    public static MemoryEstimation memoryEstimation(FastRPBaseConfig config) {
        return MemoryEstimations
            .builder(FastRP.class.getSimpleName())
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
        ProgressTracker progressTracker
    ) {
        this(graph, config, featureExtractors, progressTracker, config.randomSeed());
    }

    public FastRP(
        Graph graph,
        FastRPBaseConfig config,
        List<FeatureExtractor> featureExtractors,
        ProgressTracker progressTracker,
        Optional<Long> randomSeed
    ) {
        super(progressTracker);
        this.graph = graph;
        this.featureExtractors = featureExtractors;
        this.relationshipWeightProperty = config.relationshipWeightProperty();
        this.relationshipWeightFallback = this.relationshipWeightProperty.map(s -> Double.NaN).orElse(1.0);
        this.inputDimension = FeatureExtraction.featureCount(featureExtractors);
        this.randomSeed = improveSeed(randomSeed.orElseGet(System::nanoTime));
        this.minBatchSize = config.minBatchSize();

        this.propertyVectors = new float[inputDimension][config.propertyDimension()];
        this.embeddings = HugeObjectArray.newArray(float[].class, graph.nodeCount());
        this.embeddingA = HugeObjectArray.newArray(float[].class, graph.nodeCount());
        this.embeddingB = HugeObjectArray.newArray(float[].class, graph.nodeCount());

        this.embeddingDimension = config.embeddingDimension();
        this.baseEmbeddingDimension = config.embeddingDimension() - config.propertyDimension();
        this.iterationWeights = config.iterationWeights();
        this.nodeSelfInfluence = config.nodeSelfInfluence();
        this.normalizationStrength = config.normalizationStrength();
        this.concurrency = config.concurrency();
        this.embeddingCombiner = graph.hasRelationshipProperty()
            ? this::addArrayValuesWeighted
            : (lhs, rhs, ignoreWeight) -> addInPlace(lhs, rhs);
        this.embeddings.setAll((i) -> new float[embeddingDimension]);
    }

    @Override
    public FastRPResult compute() {
        progressTracker.beginSubTask();
        initDegreePartition();
        initPropertyVectors();
        initRandomVectors();
        addInitialVectorsToEmbedding();
        propagateEmbeddings();
        progressTracker.endSubTask();
        return new FastRPResult(embeddings);
    }

    public void initDegreePartition() {
        this.partitions = PartitionUtils.degreePartition(
            graph,
            concurrency,
            Function.identity(),
            Optional.of(minBatchSize)
        );
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
        progressTracker.beginSubTask();

        var sqrtEmbeddingDimension = (float) Math.sqrt(baseEmbeddingDimension);
        List<Runnable> tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new InitRandomVectorTask(
                partition,
                sqrtEmbeddingDimension
            ),
            Optional.of(minBatchSize)
        );
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        progressTracker.endSubTask();
    }

    void addInitialVectorsToEmbedding() {
        if (Float.compare(nodeSelfInfluence.floatValue(), 0.0f) == 0) return;
        progressTracker.beginSubTask();

        var tasks = partitions.stream()
            .map(AddInitialStateToEmbeddingTask::new)
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();
        progressTracker.endSubTask();
    }

    void propagateEmbeddings() {
        progressTracker.beginSubTask();

        for (int i = 0; i < iterationWeights.size(); i++) {
            progressTracker.beginSubTask();

            HugeObjectArray<float[]> currentEmbeddings = i % 2 == 0 ? embeddingA : embeddingB;
            HugeObjectArray<float[]> previousEmbeddings = i % 2 == 0 ? embeddingB : embeddingA;
            var iterationWeight = iterationWeights.get(i).floatValue();
            boolean firstIteration = i == 0;

            var tasks = partitions.stream()
                .map(partition -> new PropagateEmbeddingsTask(
                        partition,
                        currentEmbeddings,
                        previousEmbeddings,
                        iterationWeight,
                        firstIteration
                    )
                ).collect(Collectors.toList());
            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(tasks)
                .run();

            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();
    }

    @TestOnly
    HugeObjectArray<float[]> currentEmbedding(int iteration) {
        return iteration % 2 == 0
            ? this.embeddingA
            : this.embeddingB;
    }

    @TestOnly
    float[][] propertyVectors() {
        return propertyVectors;
    }

    @TestOnly
    HugeObjectArray<float[]> embeddings() {
        return embeddings;
    }

    private void addArrayValuesWeighted(float[] lhs, float[] rhs, double weight) {
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] = (float) Math.fma(rhs[i], weight, lhs[i]);
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

        @Override
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
        private final PropertyVectorAdder propertyVectorAdder;

        private InitRandomVectorTask(
            Partition partition,
            float sqrtEmbeddingDimension
        ) {
            this.partition = partition;
            this.sqrtEmbeddingDimension = sqrtEmbeddingDimension;
            this.propertyVectorAdder = new PropertyVectorAdder();
        }

        @Override
        public void run() {
            // this value currently doesnt matter because of reseeding below
            var random = new HighQualityRandom(randomSeed);
            partition.consume( nodeId -> {
                int degree = graph.degree(nodeId);
                float scaling = degree == 0
                    ? 1.0f
                    : (float) Math.pow(degree, normalizationStrength);

                float entryValue = scaling * sqrtSparsity / sqrtEmbeddingDimension;
                random.reseed(randomSeed ^ graph.toOriginalNodeId(nodeId));
                var randomVector = computeRandomVector(nodeId, random, entryValue);
                embeddingB.set(nodeId, randomVector);
                embeddingA.set(nodeId, new float[embeddingDimension]);
            });
            progressTracker.logProgress(partition.nodeCount());
        }

        private float[] computeRandomVector(long nodeId, Random random, float entryValue) {
            var randomVector = new float[embeddingDimension];
            for (int i = 0; i < baseEmbeddingDimension; i++) {
                randomVector[i] = computeRandomEntry(random, entryValue);
            }

            propertyVectorAdder.setRandomVector(randomVector);
            FeatureExtraction.extract(nodeId, -1, featureExtractors, propertyVectorAdder);

            return randomVector;
        }

        private class PropertyVectorAdder implements FeatureConsumer {
            private float[] randomVector;

            void setRandomVector(float[] randomVector) {
                this.randomVector = randomVector;
            }

            @Override
            public void acceptScalar(long ignored, int offset, double value) {
                float floatValue = (float) value;
                for (int i = baseEmbeddingDimension; i < embeddingDimension; i++) {
                    randomVector[i] += floatValue * propertyVectors[offset][i - baseEmbeddingDimension];
                }
            }

            @Override
            public void acceptArray(long ignored, int offset, double[] values) {
                for (int j = 0; j < values.length; j++) {
                    var value = (float) values[j];
                    float[] propertyVector = propertyVectors[offset + j];
                    for (int i = baseEmbeddingDimension; i < embeddingDimension; i++) {
                        randomVector[i] += value * propertyVector[i - baseEmbeddingDimension];
                    }
                }
            }
        }
    }

    private final class AddInitialStateToEmbeddingTask implements Runnable {
        private final Partition partition;

        private AddInitialStateToEmbeddingTask(Partition partition) {this.partition = partition;}

        @Override
        public void run() {
            partition.consume( nodeId -> {
                var initialVector = embeddingB.get(nodeId);
                var l2Norm= l2Norm( initialVector);
                float adjustedL2Norm = l2Norm < EPSILON ? 1f : l2Norm;
                addWeightedInPlace(embeddings.get(nodeId), initialVector, nodeSelfInfluence.floatValue() / adjustedL2Norm);
            });
            progressTracker.logProgress(partition.nodeCount());
        }
    }
    private final class PropagateEmbeddingsTask implements Runnable {

        private final Partition partition;
        private final HugeObjectArray<float[]> currentEmbeddings;
        private final HugeObjectArray<float[]> previousEmbeddings;
        private final float iterationWeight;
        private final Graph concurrentGraph;
        private final boolean firstIteration;

        private PropagateEmbeddingsTask(
            Partition partition,
            HugeObjectArray<float[]> currentEmbeddings,
            HugeObjectArray<float[]> previousEmbeddings,
            float iterationWeight,
            boolean firstIteration
        ) {
            this.partition = partition;
            this.currentEmbeddings = currentEmbeddings;
            this.previousEmbeddings = previousEmbeddings;
            this.iterationWeight = iterationWeight;
            this.concurrentGraph = graph.concurrentCopy();
            this.firstIteration = firstIteration;
        }

        @Override
        public void run() {
            MutableLong degrees = new MutableLong(0);
            partition.consume(nodeId -> {
                var embedding = embeddings.get(nodeId);
                var currentEmbedding = currentEmbeddings.get(nodeId);
                Arrays.fill(currentEmbedding, 0.0f);

                // Collect and combine the neighbour embeddings
                concurrentGraph.forEachRelationship(nodeId, relationshipWeightFallback, (source, target, weight) -> {
                    if (firstIteration && Double.isNaN(weight)) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Missing relationship property `%s` on relationship between nodes with ids `%d` and `%d`.",
                            relationshipWeightProperty.orElse(""),
                            graph.toOriginalNodeId(source), graph.toOriginalNodeId(target)
                        ));
                    }
                    embeddingCombiner.combine(currentEmbedding, previousEmbeddings.get(target), weight);
                    return true;
                });

                // Normalize neighbour embeddings
                var degree = graph.degree(nodeId);
                int adjustedDegree = degree == 0 ? 1 : degree;
                float degreeScale = 1.0f / adjustedDegree;
                scale(currentEmbedding, degreeScale);
                l2Normalize(currentEmbedding);

                // Update the result embedding
                addWeightedInPlace(embedding, currentEmbedding, iterationWeight);
                degrees.add(degree);
            });
            progressTracker.logProgress(degrees.longValue());
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
