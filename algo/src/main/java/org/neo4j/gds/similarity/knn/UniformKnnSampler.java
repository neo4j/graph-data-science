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

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.core.samplers.LongUniformSamplerFromRange;

import java.util.SplittableRandom;
import java.util.function.LongPredicate;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

class UniformKnnSampler implements KnnSampler {


    private final LongUniformSamplerFromRange uniformSamplerFromRange;
    private final long exclusiveMax;

    UniformKnnSampler(SplittableRandom random, long exclusiveMax) {
        this.uniformSamplerFromRange = new LongUniformSamplerFromRange(random);
        this.exclusiveMax = exclusiveMax;
    }

    public static MemoryRange memoryEstimation(long boundedK) {
        return LongUniformSamplerFromRange.memoryEstimation(boundedK)
            .add(MemoryRange.of(sizeOfInstance(UniformKnnSampler.class)));
    }

    @Override
    public long[] sample(
        long unused,
        long lowerBoundOnValidSamplesInRange,
        int numberOfSamples,
        LongPredicate isInvalidSample
    ) {
        return uniformSamplerFromRange.sample(
            0,
            exclusiveMax,
            lowerBoundOnValidSamplesInRange,
            numberOfSamples,
            isInvalidSample
        );
    }

    static class Factory implements KnnSampler.Factory {
        private final long nodeCount;
        private final SplittableRandom random;

        Factory(long nodeCount, SplittableRandom random) {
            this.nodeCount = nodeCount;
            this.random = random;
        }

        public KnnSampler create() {
            return new UniformKnnSampler(random.split(), nodeCount);
        }
    }
}
