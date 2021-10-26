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

import com.carrotsearch.hppc.LongHashSet;

import java.util.SplittableRandom;
import java.util.function.LongPredicate;
import java.util.stream.LongStream;

public class UniformSamplerWithRetries {
    private final SplittableRandom rng;
    private final LongHashSet sampledValuesCache;

    public UniformSamplerWithRetries(SplittableRandom rng) {
        this.rng = rng;
        this.sampledValuesCache = new LongHashSet();
    }

    /**
     * Samples with retries until the desired number unique samples are obtained.
     *
     * WARNING: There no maximum number of retries, so can take a long while if the number of possible samples are close
     * to the number of desired samples.
     *
     * @return array of >= max(k, lowerBoundOnValidSamplesInRange) unique samples
     */
    public long[] sample(
        long inclusiveMin,
        long exclusiveMax,
        long lowerBoundOnValidSamplesInRange,
        int numberOfSamples,
        LongPredicate isInvalidSample
    ) {
        if (numberOfSamples >= lowerBoundOnValidSamplesInRange) {
            return LongStream.range(inclusiveMin, exclusiveMax).filter(l -> !isInvalidSample.test(l)).toArray();
        }

        var samples = new long[numberOfSamples];
        int currentNumSamples = 0;

        sampledValuesCache.clear();

        while (currentNumSamples < numberOfSamples) {
            long sample = rng.nextLong(inclusiveMin, exclusiveMax);

            if (isInvalidSample.test(sample)) continue;

            if (!sampledValuesCache.add(sample)) continue;

            samples[currentNumSamples++] = sample;
        }

        return samples;
    }
}
