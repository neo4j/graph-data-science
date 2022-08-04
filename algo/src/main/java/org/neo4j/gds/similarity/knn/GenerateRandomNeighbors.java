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

import java.util.SplittableRandom;

/**
 * Initial step in KNN calculation.
 */
final class GenerateRandomNeighbors implements Runnable {
    private final KnnSampler sampler;
    private final SplittableRandom random;
    private final SimilarityFunction similarityFunction;
    private final NeighborFilter neighborFilter;
    private final HugeObjectArray<NeighborList> neighbors;
    private final int boundedK;
    private final ProgressTracker progressTracker;
    private final Partition partition;
    private final NeighbourConsumers neighbourConsumers;

    private long neighborsFound;

    GenerateRandomNeighbors(
        KnnSampler sampler,
        SplittableRandom random,
        SimilarityFunction similarityFunction,
        NeighborFilter neighborFilter,
        HugeObjectArray<NeighborList> neighbors,
        int boundedK,
        Partition partition,
        ProgressTracker progressTracker,
        NeighbourConsumers neighbourConsumers
    ) {
        this.sampler = sampler;
        this.random = random;
        this.similarityFunction = similarityFunction;
        this.neighborFilter = neighborFilter;
        this.neighbors = neighbors;
        this.boundedK = boundedK;
        this.progressTracker = progressTracker;
        this.partition = partition;
        this.neighborsFound = 0;
        this.neighbourConsumers = neighbourConsumers;
    }

    @Override
    public void run() {
        var rng = random;
        var similarityFunction = this.similarityFunction;
        var boundedK = this.boundedK;
        var neighborFilter = this.neighborFilter;

        partition.consume(nodeId -> {
            long[] chosen = sampler.sample(
                nodeId,
                neighborFilter.lowerBoundOfPotentialNeighbours(nodeId),
                boundedK,
                l -> neighborFilter.excludeNodePair(nodeId, l)
            );

            var neighbors = new NeighborList(boundedK, neighbourConsumers.get(nodeId));
            for (long candidate : chosen) {
                double similarity = similarityFunction.computeSimilarity(nodeId, candidate);
                neighbors.add(candidate, similarity, rng, 0.0);
            }

            assert neighbors.size() >= Math.min(neighborFilter.lowerBoundOfPotentialNeighbours(nodeId), boundedK);

            this.neighbors.set(nodeId, neighbors);
            neighborsFound += neighbors.size();
        });
        progressTracker.logProgress(partition.nodeCount());
    }

    long neighborsFound() {
        return neighborsFound;
    }
}
