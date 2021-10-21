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

import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.UniformSampler;

import java.util.SplittableRandom;
import java.util.stream.LongStream;

/**
 * Initial step in KNN calculation.
 */
final class GenerateRandomNeighbors implements Runnable {
    private final SplittableRandom random;
    private final SimilarityComputer computer;
    private final HugeObjectArray<NeighborList> neighbors;
    private final long nodeCount;
    private final int k;
    private final int boundedK;
    private final ProgressTracker progressTracker;
    private final Partition partition;
    private long neighborsFound;

    GenerateRandomNeighbors(
        SplittableRandom random,
        SimilarityComputer computer,
        HugeObjectArray<NeighborList> neighbors,
        long nodeCount,
        int k,
        int boundedK,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.random = random;
        this.computer = computer;
        this.neighbors = neighbors;
        this.nodeCount = nodeCount;
        this.k = k;
        this.boundedK = boundedK;
        this.progressTracker = progressTracker;
        this.partition = partition;
        this.neighborsFound = 0;
    }

    @Override
    public void run() {
        var rng = random.split();
        var computer = this.computer;
        var nodeCount = this.nodeCount;
        var k = this.k;
        var boundedK = this.boundedK;

        var uniformSampler = new UniformSampler(rng);

        partition.consume(nodeId -> {
            var validCandidates = LongStream
                .range(0, nodeCount)
                .filter(otherNode -> !computer.excludeNodePair(nodeId, otherNode));

            var chosen = uniformSampler.sample(
                validCandidates,
                computer.lowerBoundOfPotentialNeighbours(nodeId),
                boundedK
            );

            var neighbors = new NeighborList(k);
            chosen.forEach(candidate -> neighbors.add(candidate, computer.safeSimilarity(nodeId, candidate), rng));

            assert neighbors.size() == Math.min(nodeCount - 1, boundedK); // because K > 0 and N > 1
            assert neighbors.size() <= k;

            this.neighbors.set(nodeId, neighbors);
            neighborsFound += neighbors.size();
        });
        progressTracker.logProgress();
    }

    long neighborsFound() {
        return neighborsFound;
    }
}
