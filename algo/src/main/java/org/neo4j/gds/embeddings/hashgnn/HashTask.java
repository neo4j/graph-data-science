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
package org.neo4j.gds.embeddings.hashgnn;


import org.apache.commons.math3.primes.Primes;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.hashgnn.HashGNNCompanion.HashTriple.computeHashesFromTriple;

class HashTask implements Runnable {
    private final int embeddingDimension;
    private final double scaledNeighborInfluence;
    private final int numberOfRelationshipTypes;
    private final SplittableRandom rng;
    private int[] neighborsAggregationHashes;
    private int[] selfAggregationHashes;
    private List<int[]> preAggregationHashes;
    private final ProgressTracker progressTracker;

    HashTask(
        int embeddingDimension,
        double scaledNeighborInfluence,
        int numberOfRelationshipTypes,
        SplittableRandom rng,
        ProgressTracker progressTracker
    ) {
        this.embeddingDimension = embeddingDimension;
        this.scaledNeighborInfluence = scaledNeighborInfluence;
        this.numberOfRelationshipTypes = numberOfRelationshipTypes;
        this.rng = rng;
        this.progressTracker = progressTracker;
    }

    public static List<Hashes> compute(
        int embeddingDimension,
        double scaledNeighborInfluence,
        int numberOfRelationshipTypes,
        HashGNNConfig config,
        long randomSeed,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask("Precompute hashes");

        progressTracker.setSteps(config.embeddingDensity());

        var hashTasks = IntStream.range(0, config.embeddingDensity()).mapToObj(seedOffset ->
            new HashTask(
                embeddingDimension,
                scaledNeighborInfluence,
                numberOfRelationshipTypes,
                new SplittableRandom(randomSeed + seedOffset),
                progressTracker
            )).collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(hashTasks)
            .terminationFlag(terminationFlag)
            .run();

        progressTracker.endSubTask("Precompute hashes");

        return hashTasks.stream().map(HashTask::hashes).collect(Collectors.toList());
    }

    @ValueClass
    interface Hashes {
        int[] neighborsAggregationHashes();

        int[] selfAggregationHashes();

        List<int[]> preAggregationHashes();

        static long memoryEstimation(int ambientDimension, int numRelTypes) {
            long neighborAggregation = MemoryUsage.sizeOfIntArrayList(ambientDimension);
            long selfAggregation = MemoryUsage.sizeOfIntArray(ambientDimension);
            long preAggregation = MemoryUsage.sizeOfIntArrayList(numRelTypes) + MemoryUsage.sizeOfIntArray(ambientDimension) * numRelTypes;
            return neighborAggregation + selfAggregation + preAggregation + MemoryUsage.sizeOfInstance(Hashes.class);
        }
    }

    @Override
    public void run() {
        // The product of the upper bound of finalInfluence and the lower bound of primeSeed must be < Integer.MAX_VALUE
        // otherwise rng.nextInt can fail because lower bound is larger than upper bound
        double finalInfluence = Math.max(1e-4, Math.min(1e4, scaledNeighborInfluence));

        // Multiply scaling by 1.001 to make sure we can find a prime >= primeSeed * scaleFactor
        int primeSeed = rng.nextInt(
            50_000,
            (int) Math.round(Integer.MAX_VALUE / (Math.max(1, finalInfluence) * 1.001))
        );

        int neighborPrime = Primes.nextPrime(primeSeed);

        int selfPrime;
        if (Double.compare(scaledNeighborInfluence, 1.0D) == 0) {
            selfPrime = neighborPrime;
        } else {
            selfPrime = Primes.nextPrime((int) Math.round(neighborPrime * finalInfluence));
        }

        this.neighborsAggregationHashes = computeHashesFromTriple(
            embeddingDimension,
            HashGNNCompanion.HashTriple.generate(rng, neighborPrime)
        );
        this.selfAggregationHashes = computeHashesFromTriple(
            embeddingDimension,
            HashGNNCompanion.HashTriple.generate(rng, selfPrime)
        );
        this.preAggregationHashes = IntStream
            .range(0, numberOfRelationshipTypes)
            .mapToObj(unused -> computeHashesFromTriple(embeddingDimension, HashGNNCompanion.HashTriple.generate(rng)))
            .collect(Collectors.toList());

        progressTracker.logSteps(1);
    }

    Hashes hashes() {
        return ImmutableHashes.of(neighborsAggregationHashes, selfAggregationHashes, preAggregationHashes);
    }
}
