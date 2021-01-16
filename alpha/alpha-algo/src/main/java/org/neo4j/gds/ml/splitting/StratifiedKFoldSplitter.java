/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.HashSet;
import java.util.List;
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
    private final long nodeCount;
    private final HugeLongArray[] buckets;
    private final AllocationTracker allocationTracker;

    public StratifiedKFoldSplitter(int k, HugeLongArray ids, HugeLongArray targets) {
        this.buckets = makeBuckets(k, ids, targets);
        this.nodeCount = ids.size();
        this.allocationTracker = AllocationTracker.empty();
    }

    public List<NodeSplit> splits() {
        return IntStream.range(0, buckets.length).mapToObj(position -> {
            var trainSet = concatTrainSet(position);
            return NodeSplit.of(trainSet, buckets[position]);
        }).collect(Collectors.toList());
    }

    private HugeLongArray concatTrainSet(int position) {
        long size = nodeCount - buckets[position].size();
        var result = HugeLongArray.newArray(size, allocationTracker);
        var elementsAdded = 0;
        for (int i = 0; i < buckets.length; i++) {
            if (i != position) {
                for (long offset = 0; offset < buckets[i].size(); offset++) {
                    result.set(elementsAdded, buckets[i].get(offset));
                    elementsAdded++;
                }
            }
        }
        return result;
    }

    private HugeLongArray[] makeBuckets(int k, HugeLongArray ids, HugeLongArray targets) {

        var distinctClasses = new HashSet<Long>();
        for (long offset = 0; offset < targets.size(); offset++) {
            distinctClasses.add(targets.get(offset));
        }

        var nodeCount = ids.size();
        var buckets = new HugeLongArray[k];
        var bucketPositions = new int[k];

        int baseBucketSize = (int) nodeCount / k;
        for (int i = 0; i < k; i++) {
            // make the first buckets larger when nodeCount is not divisible by k
            var bucketSize = i < nodeCount % k ? baseBucketSize + 1 : baseBucketSize;
            buckets[i] = HugeLongArray.newArray(bucketSize, allocationTracker);
        }

        var roundRobinPointer = new MutableInt();
        // targets should really be integers but typed as doubles.
        // the tolerant check protects against (worry of) rounding error
        distinctClasses.forEach(currentClass -> {
            for (long offset = 0; offset < ids.size(); offset++) {
                var id = ids.get(offset);
                if (targets.get(id) == currentClass) {
                    var bucketToAddTo = roundRobinPointer.getValue();
                    buckets[bucketToAddTo].set(bucketPositions[bucketToAddTo], id);
                    bucketPositions[bucketToAddTo]++;
                    roundRobinPointer.setValue((bucketToAddTo + 1) % k);
                }
            }
        });

        return buckets;
    }
}
