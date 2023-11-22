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
package org.neo4j.gds.similarity.knn;

import java.util.List;
import java.util.Optional;

public class KnnParameters {

    static KnnParameters create(
        long nodeCount,
        int concurrency,
        int maxIterations,
        double similarityCutoff,
        double deltaThreshold,
        double sampleRate,
        int rawK,
        double perturbationRate,
        int randomJoins,
        int minBatchSize,
        KnnSampler.SamplerType samplerType,
        Optional<Long> randomSeed,
        List<KnnNodePropertySpec> nodePropertySpecs
    ) {

//        int concurrency,
        // no test atm
//        int maxIterations,
        if (maxIterations < 1) throw new IllegalArgumentException("maxIterations");
//        double similarityCutoff,
        if (Double.compare(similarityCutoff, 0.0) < 0 || Double.compare(similarityCutoff, 1.0) > 0)
            throw new IllegalArgumentException("similarityCutoff must be more than or equal to 0.0 and less than or equal to 1.0");
//        double sampleRate,
        if (Double.compare(sampleRate, 0.0) < 1 || Double.compare(sampleRate, 1.0) > 0)
            throw new IllegalArgumentException("sampleRate must be more than 0.0 and less than or equal to 1.0");
//        double deltaThreshold,
        if (Double.compare(deltaThreshold, 0.0) < 0 || Double.compare(deltaThreshold, 1.0) > 0)
            throw new IllegalArgumentException("deltaThreshold must be more than or equal to 0.0 and less than or equal to 1.0");
//        int rawK,
        if (rawK < 1) throw new IllegalArgumentException("K k must be 1 or more");

//        K k = K.create(rawK, nodeCount, sampleRate, deltaThreshold);
        // (int) is safe because k is at most `topK`, which is an int
        // upper bound for k is all other nodes in the graph
        var boundedK = Math.max(0, (int) Math.min(rawK, nodeCount - 1));
        var sampledK = Math.max(0, (int) Math.min((long) Math.ceil(sampleRate * rawK), nodeCount - 1));

        var maxUpdates = (long) Math.ceil(sampleRate * rawK * nodeCount);
        var updateThreshold = (long) Math.floor(deltaThreshold * maxUpdates);

//        double perturbationRate,
        if (Double.compare(perturbationRate, 0.0) < 0 || Double.compare(perturbationRate, 1.0) > 0)
            throw new IllegalArgumentException("perturbationRate must be more than or equal to 0.0 and less than or equal to 1.0");
//        int randomJoins,
        if (randomJoins < 0) throw new IllegalArgumentException("randomJoins must be 0 or more");

        return new KnnParameters(
            concurrency,
            maxIterations,
            similarityCutoff,
            K.create(boundedK, nodeCount, sampledK, updateThreshold),
            boundedK,
            sampledK,
            updateThreshold,
            perturbationRate,
            randomJoins,
            minBatchSize,
            samplerType,
            randomSeed,
            nodePropertySpecs
        );
    }

    private final int concurrency;
    private final int maxIterations;
    private final double similarityCutoff;
    private final K kHolder;
    private final int rawK;
    private final int sampledK;
    private final long updateThreshold;
    private final double perturbationRate;
    private final int randomJoins;
    private final int minBatchSize;
    private final KnnSampler.SamplerType samplerType;
    private final Optional<Long> randomSeed;
    private final List<KnnNodePropertySpec> nodePropertySpecs;

    public KnnParameters(
        int concurrency,
        int maxIterations,
        double similarityCutoff,
        K kHolder,
        int k,
        int sampledK,
        long updateThreshold,
        double perturbationRate,
        int randomJoins,
        int minBatchSize,
        KnnSampler.SamplerType samplerType,
        Optional<Long> randomSeed,
        List<KnnNodePropertySpec> nodePropertySpecs
    ) {
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.similarityCutoff = similarityCutoff;
        this.kHolder = kHolder;
        this.rawK = k;
        this.sampledK = sampledK;
        this.updateThreshold = updateThreshold;
        this.perturbationRate = perturbationRate;
        this.randomJoins = randomJoins;
        this.minBatchSize = minBatchSize;
        this.samplerType = samplerType;
        this.randomSeed = randomSeed;
        this.nodePropertySpecs = nodePropertySpecs;
    }

    int concurrency() {
        return concurrency;
    }

    int maxIterations() {
        return maxIterations;
    }

    double similarityCutoff() {
        return similarityCutoff;
    }

    K kHolder() {
        return kHolder;
    }

    int k() {
        return rawK;
    }

    int sampledK() {
        return sampledK;
    }

    long updateThreshold() {
        return updateThreshold;
    }

    double perturbationRate() {
        return perturbationRate;
    }

    int randomJoins() {
        return randomJoins;
    }

    int minBatchSize() {
        return minBatchSize;
    }

    KnnSampler.SamplerType samplerType() {
        return samplerType;
    }

    Optional<Long> randomSeed() {
        return randomSeed;
    }

    List<KnnNodePropertySpec> nodePropertySpecs() {
        return nodePropertySpecs;
    }
}
