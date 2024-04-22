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
package org.neo4j.gds.ml.core.samplers;

import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.SplittableRandom;
import java.util.function.IntPredicate;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;
import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;

public class IntUniformSamplerFromRange {
    public static final double RETRY_SAMPLING_RATIO = 0.6;
    private final IntUniformSamplerWithRetries retryBasedSampler;
    private final IntUniformSamplerByExclusion exclusionBasedSampler;

    public IntUniformSamplerFromRange(SplittableRandom random) {
        this.retryBasedSampler = new IntUniformSamplerWithRetries(random);
        this.exclusionBasedSampler = new IntUniformSamplerByExclusion(random);
    }

    public static MemoryRange memoryEstimation(int numberOfSamples) {
        var samplerWithRetriesEstimation = IntUniformSamplerWithRetries.memoryEstimation(numberOfSamples);
        var samplerByExclusionEstimation = IntUniformSamplerByExclusion.memoryEstimation(
            numberOfSamples,
            (long) Math.ceil(numberOfSamples / RETRY_SAMPLING_RATIO)
        );

        return samplerWithRetriesEstimation
            .add(samplerByExclusionEstimation)
            .add(MemoryRange.of(sizeOfInstance(IntUniformSamplerFromRange.class)))
            // Since only one of the samplers will be used at single point in time we can deduct the cost of one of the
            // samplers return value.
            .subtract(sizeOfLongArray(numberOfSamples));
    }

    public int[] sample(
        int inclusiveMin,
        int exclusiveMax,
        int lowerBoundOnValidSamplesInRange,
        int numberOfSamples,
        IntPredicate isInvalidSample
    ) {
        // If the number of potential neighbors are close in number to how many we want to sample, we can use retries.
        // Otherwise, we use an exclusion based method to avoid a possibly large number of retries.
        double samplingRatio = (double) numberOfSamples / lowerBoundOnValidSamplesInRange;
        if (samplingRatio < RETRY_SAMPLING_RATIO) {
            return retryBasedSampler.sample(
                inclusiveMin,
                exclusiveMax,
                lowerBoundOnValidSamplesInRange,
                numberOfSamples,
                isInvalidSample
            );
        } else {
            return exclusionBasedSampler.sample(
                inclusiveMin,
                exclusiveMax,
                lowerBoundOnValidSamplesInRange,
                numberOfSamples,
                isInvalidSample
            );
        }
    }
}
