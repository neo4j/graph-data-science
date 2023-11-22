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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.SplittableRandom;

/**
 * Initial step in KNN calculation.
 */
final class GenerateRandomNeighbors implements Runnable {

    static final class Factory {
        private final SimilarityFunction similarityFunction;
        private final NeighbourConsumers neighbourConsumers;
        private final int boundedK;
        private final ProgressTracker progressTracker;

        Factory(
            SimilarityFunction similarityFunction,
            NeighbourConsumers neighbourConsumers,
            int boundedK,
            ProgressTracker progressTracker
        ) {
            this.similarityFunction = similarityFunction;
            this.neighbourConsumers = neighbourConsumers;
            this.boundedK = boundedK;
            this.progressTracker = progressTracker;
        }

        @NotNull GenerateRandomNeighbors create(
            Partition partition,
            Neighbors neighbors,
            KnnSampler sampler,
            NeighborFilter neighborFilter,
            SplittableRandom random
        ) {
            return new GenerateRandomNeighbors(
                partition,
                neighbors,
                sampler,
                neighborFilter,
                random,
                similarityFunction,
                neighbourConsumers,
                boundedK,
                progressTracker
            );
        }
    }

    private final Partition partition;
    private final Neighbors neighbors;
    private final KnnSampler sampler;
    private final NeighborFilter neighborFilter;
    private final SplittableRandom random;
    private final SimilarityFunction similarityFunction;
    private final NeighbourConsumers neighbourConsumers;
    private final int boundedK;
    private final ProgressTracker progressTracker;

    GenerateRandomNeighbors(
        Partition partition,
        Neighbors neighbors,
        KnnSampler sampler,
        NeighborFilter neighborFilter,
        SplittableRandom random,
        SimilarityFunction similarityFunction,
        NeighbourConsumers neighbourConsumers,
        int boundedK,
        ProgressTracker progressTracker
    ) {
        this.partition = partition;
        this.neighbors = neighbors;
        this.sampler = sampler;
        this.neighborFilter = neighborFilter;
        this.random = random;
        this.similarityFunction = similarityFunction;
        this.neighbourConsumers = neighbourConsumers;
        this.boundedK = boundedK;
        this.progressTracker = progressTracker;
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
        });
        progressTracker.logProgress(partition.nodeCount());
    }
}
