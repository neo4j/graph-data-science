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
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.shuffle.ShuffleUtil;

import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.SplittableRandom;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Splits an HugeLongArray of nodes into <code>k</code> NodeSplits, each of which contains a
 * train set and a test set. Logically, the nodes are first divided into <code>k</code> nearly equal sized
 * buckets, and for each NodeSplit, one of the buckets is taken as test set and the remaining ones
 * concatenated into the train set. The split is stratified, meaning that if each node is seen as having
 * a class given by <code>targets.get(nodeId)</code>, then for each distinct class,
 * each bucket contains roughly the same number of nodes with that class.
 */
public class StratifiedKFoldSplitter {
    private final int k;
    private final ReadOnlyHugeLongArray ids;
    private final LongToLongFunction targets;
    private final SplittableRandom random;
    private final SortedSet<Long> distinctTargets;

    public static MemoryEstimation memoryEstimationForNodeSet(int k, double trainFraction) {
        return memoryEstimation(k, dim -> (long) (dim.nodeCount() * trainFraction));
    }

    public static MemoryEstimation memoryEstimation(int k, ToLongFunction<GraphDimensions> idsSetSizeExtractor) {
        return MemoryEstimations.setup("", dimensions ->  {
            var idSetSize = idsSetSizeExtractor.applyAsLong(dimensions);
            var builder = MemoryEstimations.builder(StratifiedKFoldSplitter.class.getSimpleName());
            long baseBucketSize = idSetSize / k;
            for (int fold = 0; fold < k; fold++) {
                var testSize = fold < idSetSize % k
                    ? (baseBucketSize + 1)
                    : baseBucketSize;
                var trainSize = idSetSize - testSize;

                builder.add(
                    "Fold " + fold, MemoryEstimations.builder()
                        .add(MemoryEstimations.of("Test", MemoryRange.of(HugeLongArray.memoryEstimation(testSize))))
                        .add(MemoryEstimations.of("Train", MemoryRange.of(HugeLongArray.memoryEstimation(trainSize))))
                        .build()
                );
            }
            return builder.build();
            }
        );
    }

    public StratifiedKFoldSplitter(int k, ReadOnlyHugeLongArray ids, LongToLongFunction targets, Optional<Long> randomSeed, SortedSet<Long> distinctTargets) {
        this.k = k;
        this.ids = ids;
        this.targets = targets;
        this.random = ShuffleUtil.createRandomDataGenerator(randomSeed);
        this.distinctTargets = distinctTargets;
    }

    public List<TrainingExamplesSplit> splits() {
        var nodeCount = ids.size();
        var trainSets = new HugeLongArray[k];
        var testSets = new HugeLongArray[k];
        var trainNodesAdded = new int[k];
        var testNodesAdded = new int[k];

        allocateArrays(nodeCount, trainSets, testSets);

        var roundRobinPointer = new MutableInt();
        distinctTargets.forEach(currentClass -> {
            for (long offset = 0; offset < ids.size(); offset++) {
                var id = ids.get(offset);
                if (targets.applyAsLong(id) == currentClass) {
                    var bucketToAddTo = roundRobinPointer.getValue();
                    for (int fold = 0; fold < k; fold++) {
                        if (fold == bucketToAddTo) {
                            testSets[fold].set(testNodesAdded[fold], id);
                            testNodesAdded[fold]++;
                        } else {
                            trainSets[fold].set(trainNodesAdded[fold], id);
                            trainNodesAdded[fold]++;
                        }
                    }
                    roundRobinPointer.setValue((bucketToAddTo + 1) % k);
                }
            }
        });
        return IntStream.range(0, k)
            .mapToObj(fold -> {
                ShuffleUtil.shuffleArray(trainSets[fold], random);
                ShuffleUtil.shuffleArray(testSets[fold], random);
                return TrainingExamplesSplit.of(
                    ReadOnlyHugeLongArray.of(trainSets[fold]),
                    ReadOnlyHugeLongArray.of(testSets[fold])
                );
            })
            .collect(Collectors.toList());
    }

    private void allocateArrays(long nodeCount, HugeLongArray[] trainSets, HugeLongArray[] testSets) {
        int baseBucketSize = (int) nodeCount / k;
        for (int fold = 0; fold < k; fold++) {
            // make the first buckets larger when nodeCount is not divisible by k
            var testSize = fold < nodeCount % k
                ? (baseBucketSize + 1)
                : baseBucketSize;
            testSets[fold] = HugeLongArray.newArray(testSize);
            trainSets[fold] = HugeLongArray.newArray(nodeCount - testSize);
        }
    }
}
