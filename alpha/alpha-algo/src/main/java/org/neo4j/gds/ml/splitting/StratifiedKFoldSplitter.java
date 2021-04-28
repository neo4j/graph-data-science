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
import org.apache.commons.math3.random.RandomDataGenerator;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;

/**
 * Splits an HugeLongArray of nodes into <code>k</code> NodeSplits, each of which contains a
 * train set and a test set. Logically, the nodes are first divided into <code>k</code> nearly equal sized
 * buckets, and for each NodeSplit, one of the buckets is taken as test set and the remaining ones
 * concatenated into the train set. The split is stratified, meaning that if each node is seen as having
 * a class given by <code>targets.get(nodeId)</code>, then for each distinct class,
 * each bucket contains roughly the same number of nodes with that class.
 */
public class StratifiedKFoldSplitter {
    private final AllocationTracker allocationTracker;
    private final int k;
    private final HugeLongArray targets;
    private final HugeLongArray ids;
    private final RandomDataGenerator random;

    public StratifiedKFoldSplitter(int k, HugeLongArray ids, HugeLongArray targets, Optional<Long> randomSeed) {
        this.k = k;
        this.ids = ids;
        this.targets = targets;
        this.allocationTracker = AllocationTracker.empty();
        this.random = createRandomDataGenerator(randomSeed);
    }

    public List<NodeSplit> splits() {
        var distinctClasses = distinctClasses();

        var nodeCount = ids.size();
        var trainSets = new HugeLongArray[k];
        var testSets = new HugeLongArray[k];
        var trainNodesAdded = new int[k];
        var testNodesAdded = new int[k];

        allocateArrays(nodeCount, trainSets, testSets);

        var roundRobinPointer = new MutableInt();
        distinctClasses.forEach(currentClass -> {
            for (long offset = 0; offset < ids.size(); offset++) {
                var id = ids.get(offset);
                if (targets.get(id) == currentClass) {
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
                ShuffleUtil.shuffleHugeLongArray(trainSets[fold], random);
                ShuffleUtil.shuffleHugeLongArray(testSets[fold], random);
                return NodeSplit.of(trainSets[fold], testSets[fold]);
            })
            .collect(Collectors.toList());
    }

    private void allocateArrays(long nodeCount, HugeLongArray[] trainSets, HugeLongArray[] testSets) {
        int baseBucketSize = (int) nodeCount / k;
        for (int fold = 0; fold < k; fold++) {
            // make the first buckets larger when nodeCount is not divisible by k
            var testSize = fold < nodeCount % k ? baseBucketSize + 1 : baseBucketSize;
            testSets[fold] = HugeLongArray.newArray(testSize, allocationTracker);
            trainSets[fold] = HugeLongArray.newArray(nodeCount - testSize, allocationTracker);
        }
    }

    private HashSet<Long> distinctClasses() {
        var distinctClasses = new HashSet<Long>();
        for (long offset = 0; offset < targets.size(); offset++) {
            distinctClasses.add(targets.get(offset));
        }
        return distinctClasses;
    }
}
