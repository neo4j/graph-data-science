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
package org.neo4j.gds.ml.models.randomforest;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.SplittableRandom;

final class DatasetBootstrapper {

    private DatasetBootstrapper() {}

    static ReadOnlyHugeLongArray bootstrap(
        final SplittableRandom random,
        double numFeatureVectorsRatio,
        ReadOnlyHugeLongArray trainSet,
        final BitSet bootstrappedTrainSetIndices
    ) {
        assert numFeatureVectorsRatio >= 0.0 && numFeatureVectorsRatio <= 1.0;

        long numVectors = (long) Math.ceil(numFeatureVectorsRatio * trainSet.size());
        var bootstrappedVectors = HugeLongArray.newArray(numVectors);

        for (long i = 0; i < numVectors; i++) {
            long sampledTrainingIdx = random.nextLong(0, trainSet.size());

            // we are translating the train set idx to avoid another indirection during the training
            bootstrappedVectors.set(i, trainSet.get(sampledTrainingIdx));
            bootstrappedTrainSetIndices.set(sampledTrainingIdx);
        }

        return ReadOnlyHugeLongArray.of(bootstrappedVectors);
    }
}
