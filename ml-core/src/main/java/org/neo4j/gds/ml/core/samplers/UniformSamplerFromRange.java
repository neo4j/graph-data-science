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

import java.util.SplittableRandom;
import java.util.function.LongPredicate;

public class UniformSamplerFromRange {
    public static final double RETRY_THRESHOLD = 2.0;
    private final UniformSamplerWithRetries retryBasedSampler;
    private final UniformSamplerByExclusion exclusionBasedSampler;

    public UniformSamplerFromRange(SplittableRandom random) {
        this.retryBasedSampler = new UniformSamplerWithRetries(random);
        this.exclusionBasedSampler = new UniformSamplerByExclusion(random);
    }

    public long[] sample(
        long inclusiveMin,
        long exclusiveMax,
        long lowerBoundOnValidSamplesInRange,
        int numberOfSamples,
        LongPredicate isInvalidSample
    ) {
        // If the number of potential neighbors are close in number to how many we want to sample, we can use retries.
        // Otherwise, we use an exclusion based method to avoid a possibly large number of retries.
        boolean useRetries = lowerBoundOnValidSamplesInRange > numberOfSamples * RETRY_THRESHOLD;
        if (useRetries) {
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
