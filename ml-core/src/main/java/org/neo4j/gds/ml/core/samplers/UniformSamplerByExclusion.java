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

import java.util.LinkedList;
import java.util.SplittableRandom;
import java.util.function.LongPredicate;
import java.util.stream.LongStream;

public class UniformSamplerByExclusion {
    private final UniformSamplerWithRetries samplerWithRetries;

    public UniformSamplerByExclusion(SplittableRandom rng) {
        this.samplerWithRetries = new UniformSamplerWithRetries(rng);
    }

    /**
     * Sample number by excluding from the given range. This method is appropriate to call if the amount of
     * samples one wants is not much smaller than the amount of valid numbers we sample from.
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

        var validSampleSpace = new LinkedList<Long>();
        for (long i = inclusiveMin; i < exclusiveMax; i++) {
            if (isInvalidSample.test(i)) continue;
            validSampleSpace.add(i);
        }

        assert validSampleSpace.size() >= numberOfSamples;

        var samplesToRemove = samplerWithRetries.sample(
            0,
            validSampleSpace.size(),
            validSampleSpace.size(),
            validSampleSpace.size() - numberOfSamples,
            __ -> false
        );

        for (long i : samplesToRemove) {
            // Safe since samplesToRemove contain indices of an integer-indexed list
            validSampleSpace.remove(i);
        }

        return validSampleSpace.stream().mapToLong(l -> l).toArray();
    }
}
