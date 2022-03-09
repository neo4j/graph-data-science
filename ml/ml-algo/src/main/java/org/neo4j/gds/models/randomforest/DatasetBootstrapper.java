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
package org.neo4j.gds.models.randomforest;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.SplittableRandom;

final class DatasetBootstrapper {

    private DatasetBootstrapper() {}

    static HugeLongArray bootstrap(
        final SplittableRandom random,
        double numFeatureVectorsRatio,
        long totalNumVectors,
        final BitSet cachedBootstrappedDataset
    ) {
        assert numFeatureVectorsRatio >= 0.0 && numFeatureVectorsRatio <= 1.0;

        long numVectors = (long) Math.ceil(numFeatureVectorsRatio * totalNumVectors);
        var bootstrappedVectors = HugeLongArray.newArray(numVectors);

        for (long i = 0; i < numVectors; i++) {
            long sampledVectorIdx = random.nextLong(0, totalNumVectors);
            bootstrappedVectors.set(i, sampledVectorIdx);
            cachedBootstrappedDataset.set(sampledVectorIdx);
        }

        return bootstrappedVectors;
    }
}
