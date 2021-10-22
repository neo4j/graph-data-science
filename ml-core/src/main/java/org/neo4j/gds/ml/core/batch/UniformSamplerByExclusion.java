/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.gds.ml.core.batch;

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
