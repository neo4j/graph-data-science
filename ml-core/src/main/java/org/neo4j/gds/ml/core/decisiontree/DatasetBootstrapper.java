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
package org.neo4j.gds.ml.core.decisiontree;

import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Random;

final class DatasetBootstrapper {

    private DatasetBootstrapper() {}

    static HugeLongArray bootstrap(
        final Random random,
        double numFeatureVectorsRatio,
        final HugeByteArray cachedBootstrappedDataset,
        AllocationTracker allocationTracker
    ) {
        assert numFeatureVectorsRatio >= 0.0 && numFeatureVectorsRatio <= 1.0;
        assert cachedBootstrappedDataset.size() > 0;

        final long totalNumFeatureVectors = cachedBootstrappedDataset.size();
        final long numVectors = (long) Math.ceil(numFeatureVectorsRatio * totalNumFeatureVectors);
        final var bootstrappedVectors = HugeLongArray.newArray(numVectors, allocationTracker);

        for (long i = 0; i < numVectors; i++) {
            long j = randomNonNegativeLong(random, 0, totalNumFeatureVectors);
            bootstrappedVectors.set(i, j);
            cachedBootstrappedDataset.set(j, (byte) 1);
        }

        return bootstrappedVectors;
    }

    // Handle that `Math.abs(Long.MIN_VALUE) == Long.MIN_VALUE`.
    // `min` is inclusive, and `max` is exclusive.
    private static long randomNonNegativeLong(Random random, long min, long max) {
        assert min >= 0;
        assert max > min;

        long randomNum;
        do {
            randomNum = random.nextLong();
        } while (randomNum == Long.MIN_VALUE);

        return (Math.abs(randomNum) % (max - min)) + min;
    }
}
