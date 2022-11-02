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

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.hashgnn.HashGNNCompanion.HashTriple.computeHashesFromTriple;

class HashTask implements Runnable {
    private final int embeddingDimension;
    private final double scaledNeighborInfluence;
    private final int numberOfRelationships;
    private final SplittableRandom rng;
    private int[] neighborsAggregationHashes;
    private int[] selfAggregationHashes;
    private List<int[]> preAggregationHashes;

    HashTask(
        int embeddingDimension,
        double scaledNeighborInfluence,
        int numberOfRelationships,
        SplittableRandom rng
    ) {
        this.embeddingDimension = embeddingDimension;
        this.scaledNeighborInfluence = scaledNeighborInfluence;
        this.numberOfRelationships = numberOfRelationships;
        this.rng = rng;
    }

    public static List<Hashes> compute(
        int embeddingDimension,
        double scaledNeighborInfluence,
        int numberOfRelationships,
        HashGNNConfig config,
        long randomSeed,
        TerminationFlag terminationFlag
    ) {
        var hashTasks = IntStream.range(0, config.iterations() * config.embeddingDensity()).mapToObj(seedOffset ->
            new HashTask(
                embeddingDimension,
                scaledNeighborInfluence,
                numberOfRelationships,
                new SplittableRandom(randomSeed + seedOffset)
            )).collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(hashTasks)
            .terminationFlag(terminationFlag)
            .run();

        var hashes = new ArrayList<Hashes>(config.iterations() * config.embeddingDensity());
        hashTasks.forEach(hashTask -> hashes.add(hashTask.hashes()));

        return hashes;
    }

    @ValueClass
    interface Hashes {
        int[] neighborsAggregationHashes();

        int[] selfAggregationHashes();

        List<int[]> preAggregationHashes();
    }

    @Override
    public void run() {
        double finalInfluence = Math.max(1e-5, Math.min(1e5, scaledNeighborInfluence));

        // Multiply scaling by 1.001 to make sure we can find a prime >= primeSeed * scaleFactor
        int primeSeed = rng.nextInt(1_000_000, (int) Math.round(Integer.MAX_VALUE / (Math.max(1, finalInfluence) * 1.001)));

        int c = Primes.nextPrime(primeSeed);

        int d;
        if (Double.compare(scaledNeighborInfluence, 1.0D) == 0) {
            d = c;
        } else {
            d = Primes.nextPrime((int) Math.round(c * finalInfluence));
        }

        this.neighborsAggregationHashes = computeHashesFromTriple(
            embeddingDimension,
            HashGNNCompanion.HashTriple.generate(rng, c)
        );
        this.selfAggregationHashes = computeHashesFromTriple(embeddingDimension, HashGNNCompanion.HashTriple.generate(rng, d));
        this.preAggregationHashes = IntStream
            .range(0, numberOfRelationships)
            .mapToObj(unused -> computeHashesFromTriple(embeddingDimension, HashGNNCompanion.HashTriple.generate(rng)))
            .collect(Collectors.toList());
    }

    Hashes hashes() {
        return ImmutableHashes.of(neighborsAggregationHashes, selfAggregationHashes, preAggregationHashes);
    }
}
