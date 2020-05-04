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
package org.neo4j.graphalgo.impl.embedding;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RandomProjection extends Algorithm<RandomProjection, RandomProjection> {

    private final Graph graph;
    private final int concurrency;
    private final boolean normalizeL2;
    private final float normalizationStrength;
    private final HugeObjectArray<float[]> embeddings;
    private final HugeObjectArray<float[]> embeddingA;
    private final HugeObjectArray<float[]> embeddingB;

    private final int embeddingDimension;
    private final int sparsity;
    private final int iterations;
    private final int seed;
    private final List<Double> iterationWeights;

    // TODO use config instead of single arguments
    public RandomProjection(
        Graph graph,
        int embeddingDimension,
        int sparsity,
        int iterations,
        List<Double> iterationWeights,
        float normalizationStrength,
        boolean normalizeL2,
        int seed,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.graph = graph;

        this.embeddings = HugeObjectArray.newArray(float[].class, graph.nodeCount(), tracker);
        this.embeddingA = HugeObjectArray.newArray(float[].class, graph.nodeCount(), tracker);
        this.embeddingB = HugeObjectArray.newArray(float[].class, graph.nodeCount(), tracker);

        this.embeddingDimension = embeddingDimension;
        this.sparsity = sparsity;
        this.iterations = iterations;
        this.iterationWeights = iterationWeights;
        this.normalizationStrength = normalizationStrength;
        this.normalizeL2 = normalizeL2;
        this.seed = seed;
        this.concurrency = concurrency;

        int embeddingSize = iterationWeights.isEmpty() ? embeddingDimension * iterations : embeddingDimension;
        this.embeddings.setAll((i) -> new float[embeddingSize]);
    }

    @Override
    public RandomProjection compute() {
        initRandomVectors();
        propagateEmbeddings();
        return me();
    }

    public HugeObjectArray<float[]> embeddings() {
        return this.embeddings;
    }

    HugeObjectArray<float[]> currentEmbedding(int iteration) {
        return iteration % 2 == 0
            ? this.embeddingA
            : this.embeddingB;
    }

    @Override
    public RandomProjection me() {
        return this;
    }

    @Override
    public void release() {
        this.embeddingA.release();
        this.embeddingB.release();
    }

    void initRandomVectors() {
        float probability = 1.0f / (2.0f * sparsity);
        float sqrtSparsity = (float) Math.sqrt(sparsity);
        float sqrtEmbeddingDimension = (float) Math.sqrt(embeddingDimension);
        Random random = new HighQualityRandom(seed);

        ParallelUtil.parallelForEachNode(graph, concurrency, nodeId -> {
            int degree = graph.degree(nodeId);
            float scaling = degree == 0
                ? 1.0f
                : (float) Math.pow(degree, normalizationStrength);

            float entryValue = scaling * sqrtSparsity / sqrtEmbeddingDimension;
            float[] randomVector = computeRandomVector(random, probability, entryValue);
            embeddingB.set(nodeId, randomVector);
        });
    }

    void propagateEmbeddings() {
        for (int i = 0; i < iterations; i++) {
            var localCurrent = i % 2 == 0 ? embeddingA : embeddingB;
            var localPrevious = i % 2 == 0 ? embeddingB : embeddingA;

            ParallelUtil.parallelForEachNode(graph, concurrency, nodeId -> {
                float[] currentEmbedding = new float[embeddingDimension];
                localCurrent.set(nodeId, currentEmbedding);
                graph.concurrentCopy().forEachRelationship(nodeId, (source, target) -> {
                    addArrayValues(currentEmbedding, localPrevious.get(target));
                    return true;
                });

                int degree = graph.degree(nodeId) == 0 ? 1 : graph.degree(nodeId);
                float degreeScale = 1.0f / degree;
                multiplyArrayValues(currentEmbedding, degreeScale);
            });

            int offset = embeddingDimension * i;
            double weight = iterationWeights.isEmpty()
                ? Double.NaN
                : iterationWeights.get(i);
            ParallelUtil.parallelForEachNode(graph, concurrency, nodeId -> {
                float[] embedding = embeddings.get(nodeId);

                float[] newEmbedding = localCurrent.get(nodeId);
                if (normalizeL2) {
                    l2Normalize(newEmbedding);
                }
                if (iterationWeights.isEmpty()) {
                    System.arraycopy(newEmbedding, 0, embedding, offset, embeddingDimension);
                } else {
                    updateEmbeddings((float) weight, embedding, newEmbedding);
                }
            });
        }
    }

    private float[] computeRandomVector(Random random, float probability, float entryValue) {
        float[] randomVector = new float[embeddingDimension];
        for (int i = 0; i < embeddingDimension; i++) {
            randomVector[i] = computeRandomEntry(random, probability, entryValue);
        }
        return randomVector;
    }

    private float computeRandomEntry(Random random, float probability, float entryValue) {
        float randomValue = random.nextFloat();

        if (randomValue < probability) {
            return entryValue;
        } else if (randomValue < probability * 2.0f) {
            return -entryValue;
        } else {
            return 0.0f;
        }
    }

    private void updateEmbeddings(float weight, float[] embedding, float[] newEmbedding) {
        multiplyArrayValues(newEmbedding, weight);
        addArrayValues(embedding, newEmbedding);
    }

    private void addArrayValues(float[] lhs, float[] rhs) {
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] += rhs[i];
        }
    }

    private void multiplyArrayValues(float[] lhs, float scalar) {
        for (int i = 0; i < lhs.length; i++) {
            lhs[i] *= scalar;
        }
    }

    private void l2Normalize(float[] array) {
        float sum = 0.0f;
        for (float value : array) {
            sum += value * value;
        }
        float sqrtSum = (float) (sum == 0 ? 1 : Math.sqrt(sum));
        float scaling = 1 / sqrtSum;
        for (int i = 0; i < array.length; i++) {
            array[i] *= scaling;
        }
    }

    public class HighQualityRandom extends Random {
        private Lock l = new ReentrantLock();
        private long u;
        private long v = 4101842887655102017L;
        private long w = 1;

        public HighQualityRandom() {
            this(System.nanoTime());
        }
        public HighQualityRandom(long seed) {
            l.lock();
            u = seed ^ v;
            nextLong();
            v = u;
            nextLong();
            w = v;
            nextLong();
            l.unlock();
        }

        public long nextLong() {
            l.lock();
            try {
                u = u * 2862933555777941757L + 7046029254386353087L;
                v ^= v >>> 17;
                v ^= v << 31;
                v ^= v >>> 8;
                w = 4294957665L * (w & 0xffffffff) + (w >>> 32);
                long x = u ^ (u << 21);
                x ^= x >>> 35;
                x ^= x << 4;
                long ret = (x + v) ^ w;
                return ret;
            } finally {
                l.unlock();
            }
        }

        protected int next(int bits) {
            return (int) (nextLong() >>> (64-bits));
        }
    }
}