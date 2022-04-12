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
package org.neo4j.gds.ml.splitting;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;

/**
 * Splits an HugeLongArray of nodes into <code>k</code> NodeSplits, each of which contains a
 * train set and a test set. Logically, the nodes are first divided into <code>k</code> nearly equal sized
 * buckets, and for each NodeSplit, one of the buckets is taken as test set and the remaining ones
 * concatenated into the train set.
 */
public class KFoldSplitter {
    private final int k;
    private final ReadOnlyHugeLongArray ids;

    public static MemoryEstimation memoryEstimationForNodeSet(int k, double trainFraction) {
        return memoryEstimation(k, dim -> (long) (dim.nodeCount() * trainFraction));
    }

    private static MemoryEstimation memoryEstimation(int k, ToLongFunction<GraphDimensions> idsSetSizeExtractor) {
        return MemoryEstimations.setup("", dimensions ->  {
            var idSetSize = idsSetSizeExtractor.applyAsLong(dimensions);
            var builder = MemoryEstimations.builder(KFoldSplitter.class);
            long baseBucketSize = idSetSize / k;
            for (int fold = 0; fold < k; fold++) {
                var testSize = fold < idSetSize % k
                    ? (baseBucketSize + 1)
                    : baseBucketSize;
                var test = HugeLongArray.memoryEstimation(testSize);
                var train = HugeLongArray.memoryEstimation(idSetSize - testSize);

                builder.add(MemoryEstimations.of(
                    "Fold" + fold,
                    MemoryRange.of(HugeLongArray.memoryEstimation(test) + HugeLongArray.memoryEstimation(train))
                ));
            }
            return builder.build();
            }
        );
    }

    public KFoldSplitter(int k, ReadOnlyHugeLongArray shuffledIds) {
        this.k = k;
        this.ids = shuffledIds;
    }

    public List<TrainingExamplesSplit> splits() {
        var splits = new ArrayList<TrainingExamplesSplit>(k);
        long totalSetSize = ids.size();

        var idOffset = new MutableInt(0);
        for (int i = 0; i < k; i++) {
            // one bucket -> testSet, rest buckets as trainSet
            var testSize = testSize(k);
            var trainSize = totalSetSize - testSize;

            var trainSet = HugeLongArray.newArray(trainSize);
            var testSet = HugeLongArray.newArray(testSize);

            // we take ranges as we expect the ids to be shuffled

            // kth fold as test set
            testSet.setAll(idx -> ids.get(idOffset.getValue() + idx));

            // k-1 folds as train set
            idOffset.add(testSize);
            trainSet.setAll(idx -> ids.get((idOffset.getValue() + idx) % totalSetSize));


            splits.add(TrainingExamplesSplit.of(trainSet, testSet));
        }

        return splits;
    }

    private long testSize(int fold) {
        var baseBucketSize = ids.size() / k;

        // make the first buckets larger when nodeCount is not divisible by k
        return fold < (ids.size() % k)
            ? (baseBucketSize + 1)
            : baseBucketSize;
    }
}
