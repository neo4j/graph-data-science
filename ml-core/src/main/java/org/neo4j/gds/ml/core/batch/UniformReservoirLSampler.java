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
package org.neo4j.gds.ml.core.batch;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.LongStream;

/*
 * L Algorithm for uniform sampling k elements from an input stream
 * https://richardstartin.github.io/posts/reservoir-sampling#algorithm-l
 *
 */
public class UniformReservoirLSampler {

    private final Random random;

    public UniformReservoirLSampler(long randomSeed) {
        this.random = new Random(randomSeed);
    }

    public LongStream sample(LongStream input, long inputSize, int numberOfSamples) {
        if (numberOfSamples == 0) {
            return LongStream.empty();
        }

        if (numberOfSamples >= inputSize) {
            return input;
        }

        long[] reservoir = new long[numberOfSamples];

        var inputIterator = input.iterator();

        for (int i = 0; i < numberOfSamples; i++) {
            reservoir[i] = inputIterator.nextLong();
        }

        var nextIdxToSample = numberOfSamples - 1;
        // `w` in original Algorithm L
        var skipFactor = computeSkipFactor(numberOfSamples);

        // compute first skip
        nextIdxToSample += computeNumberOfSkips(skipFactor);
        skipFactor *= computeSkipFactor(numberOfSamples);

        for(int idx = numberOfSamples; inputIterator.hasNext(); idx++) {
            var inputValue = inputIterator.nextLong();
            if (idx == nextIdxToSample) {
                reservoir[random.nextInt(numberOfSamples)] = inputValue;
                // compute next value
                nextIdxToSample += computeNumberOfSkips(skipFactor);
                skipFactor *= computeSkipFactor(numberOfSamples);
            }
        }

        return Arrays.stream(reservoir);
    }

    private double computeSkipFactor(int numberOfSamples) {
        return Math.exp(Math.log(random.nextDouble()) / numberOfSamples);
    }

    private long computeNumberOfSkips(double w) {
        return (long) (Math.log(random.nextDouble()) / Math.log(1 - w)) + 1;
    }
}
