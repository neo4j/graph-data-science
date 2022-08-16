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

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.core.samplers.LongUniformSamplerFromRange;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.Optional;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.LongPredicate;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongHashSet;

class RandomWalkKnnSampler implements KnnSampler {

    // Extend the walks a bit to compensate for possibly sampling duplicate nodes in the walks.
    private static final int WALK_LENGTH_MULTIPLIER = 3;

    private final RandomWalkSampler randomWalkSampler;
    private final LongUniformSamplerFromRange uniformSamplerFromRange;
    private final long exclusiveMax;
    private final LongHashSet sampledValuesCache;

    RandomWalkKnnSampler(
        Graph graph,
        SplittableRandom random,
        // Since RandomWalk seeds per node the RandomWalkSampler can't take a SplittableRandom.
        Optional<Long> randomSeed,
        int k
    ) {
        assert k > 0;

        this.randomWalkSampler = new RandomWalkSampler(
            graph::degree,
            WALK_LENGTH_MULTIPLIER * k,
            // Prefer deeper walks.
            0.4,
            0.6,
            1.0,
            graph,
           randomSeed.orElseGet(() -> new Random().nextLong())
        );
        this.uniformSamplerFromRange = new LongUniformSamplerFromRange(random);
        this.exclusiveMax = graph.nodeCount();
        this.sampledValuesCache = new LongHashSet();
    }

    public static MemoryRange memoryEstimation(long boundedK) {
        var baseEstimation = RandomWalkSampler.memoryEstimation(boundedK * WALK_LENGTH_MULTIPLIER)
            .add(MemoryRange.of(
                sizeOfInstance(RandomWalkKnnSampler.class) +
                sizeOfLongArray(boundedK) +
                sizeOfLongHashSet(boundedK)
            ));

        return baseEstimation
            .add(LongUniformSamplerFromRange.memoryEstimation(0))
            .union(baseEstimation.add(LongUniformSamplerFromRange.memoryEstimation(boundedK)));
    }

    @Override
    public long[] sample(
        long nodeId,
        long lowerBoundOnValidSamplesInRange,
        final int numberOfSamples,
        final LongPredicate isInvalidSample
    ) {

        final var walk = randomWalkSampler.walk(nodeId);

        sampledValuesCache.clear();
        final var samples = new long[numberOfSamples];
        int addedSamples = 0;
        for (int i = 1; i < walk.length; i++) {
            long node = walk[i];

            if (isInvalidSample.test(node)) {
                continue;
            }

            if (sampledValuesCache.contains(node)) {
                continue;
            }

            sampledValuesCache.add(node);
            samples[addedSamples++] = node;

            if (addedSamples == numberOfSamples) {
                return samples;
            }
        }

        // Fill up with uniformly random nodes if walk did not contain enough unique valid sample candidates.
        var uniformSamples = uniformSamplerFromRange.sample(
            0,
            exclusiveMax,
            lowerBoundOnValidSamplesInRange - addedSamples,
            numberOfSamples - addedSamples,
            node -> isInvalidSample.test(node) || sampledValuesCache.contains(node)
        );

        System.arraycopy(uniformSamples, 0, samples, addedSamples, uniformSamples.length);

        return samples;
    }
}
