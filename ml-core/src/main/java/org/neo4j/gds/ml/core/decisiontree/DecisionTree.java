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

import java.util.stream.IntStream;

public abstract class DecisionTree<L extends DecisionTreeLoss, P> {

    private final L lossFunction;
    private final double[][] allFeatures;
    private final int maxDepth;
    private final int minSize;

    public DecisionTree(
        L lossFunction,
        double[][] allFeatures,
        int maxDepth,
        int minSize
    ) {
        assert allFeatures.length > 0;
        assert maxDepth >= 1;
        assert minSize >= 0;

        this.lossFunction = lossFunction;
        this.allFeatures = allFeatures;
        this.maxDepth = maxDepth;
        this.minSize = minSize;
    }

    public TreeNode train() {
        var rootSplit = bestSplit(IntStream.range(0, allFeatures.length).toArray(), allFeatures.length);

        return split(rootSplit, 1);
    }

    public abstract P predict(TreeNode node, double[] features);

    protected abstract P toTerminal(int[] group, int groupSize);

    private int[] computeSplit(
        int index,
        double value,
        int[] group,
        int groupSize,
        int[][] childGroups
    ) {
        assert groupSize > 0;
        assert group.length >= groupSize;
        assert childGroups.length == 2;
        assert index >= 0 && index < allFeatures[0].length;

        int leftGroupSize = 0;
        int rightGroupSize = 0;

        for (int i = 0; i < groupSize; i++) {
            var featuresIdx = group[i];
            var features = allFeatures[featuresIdx];
            if (features[index] < value) {
                childGroups[0][leftGroupSize++] = featuresIdx;
            } else {
                childGroups[1][rightGroupSize++] = featuresIdx;
            }
        }

        return new int[]{leftGroupSize, rightGroupSize};
    }

    private Split bestSplit(int[] group, int groupSize) {
        assert groupSize > 0;
        assert group.length >= groupSize;

        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestLoss = Double.MAX_VALUE;

        int[][] childGroups = {new int[groupSize], new int[groupSize]};
        int[][] bestChildGroups = {new int[groupSize], new int[groupSize]};
        int[] bestGroupSizes = {-1, -1};

        for (int i = 0; i < allFeatures[0].length; i++) {
            for (int j = 0; j < groupSize; j++) {
                var features = allFeatures[group[j]];

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
