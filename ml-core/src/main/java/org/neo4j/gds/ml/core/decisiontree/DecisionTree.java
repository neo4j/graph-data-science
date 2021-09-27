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
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

public abstract class DecisionTree<L extends DecisionTreeLoss, P> {

    private final AllocationTracker allocationTracker;
    private final L lossFunction;
    private final HugeObjectArray<double[]> allFeatures;
    private final int maxDepth;
    private final int minSize;

    public DecisionTree(
        AllocationTracker allocationTracker,
        L lossFunction,
        HugeObjectArray<double[]> allFeatures,
        int maxDepth,
        int minSize
    ) {
        assert allFeatures.size() > 0;
        assert maxDepth >= 1;
        assert minSize >= 0;

        this.allocationTracker = allocationTracker;
        this.lossFunction = lossFunction;
        this.allFeatures = allFeatures;
        this.maxDepth = maxDepth;
        this.minSize = minSize;
    }

    public TreeNode train() {
        var startGroup = HugeLongArray.newArray(allFeatures.size(), allocationTracker);
        startGroup.setAll(i -> i);

        var rootSplit = bestSplit(startGroup, startGroup.size());

        return split(rootSplit, 1);
    }

    public abstract P predict(TreeNode node, double[] features);

    protected abstract P toTerminal(HugeLongArray group, long groupSize);

    private long[] computeSplit(
        int index,
        double value,
        HugeLongArray group,
        long groupSize,
        HugeLongArray[] childGroups
    ) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert childGroups.length == 2;
        assert index >= 0 && index < allFeatures.get(0).length;

        long leftGroupSize = 0;
        long rightGroupSize = 0;

        for (int i = 0; i < groupSize; i++) {
            var featuresIdx = group.get(i);
            var features = allFeatures.get(featuresIdx);
            if (features[index] < value) {
                childGroups[0].set(leftGroupSize++, featuresIdx);
            } else {
                childGroups[1].set(rightGroupSize++, featuresIdx);
            }
        }

        return new long[]{leftGroupSize, rightGroupSize};
    }

    private Split bestSplit(HugeLongArray group, long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestLoss = Double.MAX_VALUE;

        HugeLongArray[] childGroups = {
            HugeLongArray.newArray(groupSize, allocationTracker),
            HugeLongArray.newArray(groupSize, allocationTracker)
        };
        HugeLongArray[] bestChildGroups = {
            HugeLongArray.newArray(groupSize, allocationTracker),
            HugeLongArray.newArray(groupSize, allocationTracker)
        };
        long[] bestGroupSizes = {-1, -1};

        for (int i = 0; i < allFeatures.get(0).length; i++) {
            for (int j = 0; j < groupSize; j++) {
                var features = allFeatures.get(group.get(j));

                var groupSizes = computeSplit(i, features[i], group, groupSize, childGroups);

                var loss = lossFunction.splitLoss(childGroups, groupSizes);

                if (loss < bestLoss) {
                    bestIdx = i;
                    bestValue = features[i];
                    bestLoss = loss;

                    var tmpGroups = bestChildGroups;
                    bestChildGroups = childGroups;
                    childGroups = tmpGroups;

                    bestGroupSizes = groupSizes;
                }
            }
        }

        return ImmutableSplit.of(
            bestIdx,
            bestValue,
            bestChildGroups[0],
            bestChildGroups[1],
            bestGroupSizes[0],
            bestGroupSizes[1]
        );
    }

    private TreeNode split(Split split, int depth) {
        assert depth <= maxDepth;
        assert split.leftGroupSize() + split.rightGroupSize() > 0;

        if (split.leftGroupSize() == 0) {
            return ImmutableLeafNode.of(toTerminal(split.rightGroup(), split.rightGroupSize()));
        }

        if (split.rightGroupSize() == 0) {
            return ImmutableLeafNode.of(toTerminal(split.leftGroup(), split.leftGroupSize()));
        }

        if (depth >= maxDepth) {
            return ImmutableNonLeafNode.of(
                split.index(),
                split.value(),
                ImmutableLeafNode.of(toTerminal(split.leftGroup(), split.leftGroupSize())),
                ImmutableLeafNode.of(toTerminal(split.rightGroup(), split.rightGroupSize()))
            );
        }

        TreeNode leftChild;
        if (split.leftGroupSize() <= minSize) {
            leftChild = ImmutableLeafNode.of(toTerminal(split.leftGroup(), split.leftGroupSize()));
        } else {
            var leftSplit = bestSplit(split.leftGroup(), split.leftGroupSize());
            leftChild = split(leftSplit, depth + 1);
        }

        TreeNode rightChild;
        if (split.rightGroupSize() <= minSize) {
            rightChild = ImmutableLeafNode.of(toTerminal(split.rightGroup(), split.rightGroupSize()));
        } else {
            var rightSplit = bestSplit(split.rightGroup(), split.rightGroupSize());
            rightChild = split(rightSplit, depth + 1);
        }

        return ImmutableNonLeafNode.of(
            split.index(),
            split.value(),
            leftChild,
            rightChild
        );
    }
}
