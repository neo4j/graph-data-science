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

import com.carrotsearch.hppc.IntHashSet;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.SplittableRandom;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;
import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;
import static org.neo4j.gds.mem.Estimate.sizeOfLongHashSet;

public class IntUniformSamplerWithRetries {
    private final SplittableRandom rng;
    private final IntHashSet sampledValuesCache;

    public IntUniformSamplerWithRetries(SplittableRandom rng) {
        this.rng = rng;
        this.sampledValuesCache = new IntHashSet();
    }

    public static MemoryRange memoryEstimation(long numberOfSamples) {
        return MemoryRange.of(
            sizeOfInstance(IntUniformSamplerWithRetries.class) +
            sizeOfLongHashSet(numberOfSamples) +
            sizeOfLongArray(numberOfSamples)
        );
    }

    /**
     * Samples with retries until the desired number unique samples are obtained.
     *
     * WARNING: There no maximum number of retries, so can take a long while if the number of possible samples are close
     * to the number of desired samples.
     *
     * @return array of {@literal >=} max(k, lowerBoundOnValidSamplesInRange) unique samples
     */
    public int[] sample(
        int inclusiveMin,
        int exclusiveMax,
        int lowerBoundOnValidSamplesInRange,
        int numberOfSamples,
        IntPredicate isInvalidSample
    ) {
        if (numberOfSamples >= lowerBoundOnValidSamplesInRange) {
            return IntStream.range(inclusiveMin, exclusiveMax).filter(l -> !isInvalidSample.test(l)).toArray();
        }

        var samples = new int[numberOfSamples];
        int currentNumSamples = 0;

        sampledValuesCache.clear();

        while (currentNumSamples < numberOfSamples) {
            int sample = rng.nextInt(inclusiveMin, exclusiveMax);

            if (isInvalidSample.test(sample)) continue;

            if (!sampledValuesCache.add(sample)) continue;

            samples[currentNumSamples++] = sample;
        }

        return samples;
    }
}
