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
package org.neo4j.gds.decisiontree;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.ArrayDeque;
import java.util.Deque;

public abstract class DecisionTreeTrain<LOSS extends DecisionTreeLoss, PREDICTION> {

    private final LOSS lossFunction;
    private final HugeObjectArray<double[]> allFeatureVectors;
    private final int maxDepth;
    private final int minSize;
    private final FeatureBagger featureBagger;

    DecisionTreeTrain(
        HugeObjectArray<double[]> allFeatureVectors,
        DecisionTreeTrainConfig config,
        LOSS lossFunction,
        FeatureBagger featureBagger
    ) {
        assert allFeatureVectors.size() > 0;

        this.lossFunction = lossFunction;
        this.allFeatureVectors = allFeatureVectors;
        this.maxDepth = config.maxDepth();
        this.minSize = config.minSplitSize();

        this.featureBagger = featureBagger;
    }

    public DecisionTreePredict<PREDICTION> train(HugeLongArray sampledFeatureVectors) {
        var stack = new ArrayDeque<StackRecord<PREDICTION>>();
        TreeNode<PREDICTION> root;

        root = splitAndPush(stack, sampledFeatureVectors, sampledFeatureVectors.size(), 1);

        while (!stack.isEmpty()) {
            var record = stack.pop();
            var split = record.split();

            if (record.depth() >= maxDepth || split.sizes().left() <= minSize) {
                record.node().leftChild = new TreeNode<>(toTerminal(split.groups().left(), split.sizes().left()));
            } else {
                record.node().leftChild = splitAndPush(
                    stack,
                    split.groups().left(),
                    split.sizes().left(),
                    record.depth() + 1
                );
            }

            if (record.depth() >= maxDepth || split.sizes().right() <= minSize) {
                record.node().rightChild = new TreeNode<>(toTerminal(split.groups().right(), split.sizes().right()));
            } else {
                record.node().rightChild = splitAndPush(
                    stack,
                    split.groups().right(),
                    split.sizes().right(),
                    record.depth() + 1
                );
            }
        }

        return new DecisionTreePredict<>(root);
    }

    // TODO comment to explain this name could be useful
    protected abstract PREDICTION toTerminal(HugeLongArray group, long groupSize);

    private TreeNode<PREDICTION> splitAndPush(
        Deque<StackRecord<PREDICTION>> stack,
        HugeLongArray group,
        long groupSize,
        int depth
    ) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert depth >= 1;

        var split = findBestSplit(group, groupSize);
        if (split.sizes().right() == 0) {
            return new TreeNode<>(toTerminal(split.groups().left(), split.sizes().right()));
        } else if (split.sizes().left() == 0) {
            return new TreeNode<>(toTerminal(split.groups().right(), split.sizes().right()));
        }

        var node = new TreeNode<PREDICTION>(split.index(), split.value());
        stack.push(ImmutableStackRecord.of(node, split, depth));

        return node;
    }

    private GroupSizes createSplit(
        final int index,
        final double value,
        HugeLongArray group,
        final long groupSize,
        Groups groups
    ) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert index >= 0 && index < allFeatureVectors.get(0).length;

        long leftGroupSize = 0;
        long rightGroupSize = 0;

        final var leftGroup = groups.left();
        final var rightGroup = groups.right();

        for (int i = 0; i < groupSize; i++) {
            var featuresIdx = group.get(i);
            var features = allFeatureVectors.get(featuresIdx);
            if (features[index] < value) {
                leftGroup.set(leftGroupSize++, featuresIdx);
            } else {
                rightGroup.set(rightGroupSize++, featuresIdx);
            }
        }

        return ImmutableGroupSizes.of(leftGroupSize, rightGroupSize);
    }

    private Split findBestSplit(final HugeLongArray group, final long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestLoss = Double.MAX_VALUE;

        var childGroups = ImmutableGroups.of(
            HugeLongArray.newArray(groupSize),
            HugeLongArray.newArray(groupSize)
        );
        var bestChildGroups = ImmutableGroups.of(
            HugeLongArray.newArray(groupSize),
            HugeLongArray.newArray(groupSize)
        );
        var bestGroupSizes = ImmutableGroupSizes.of(-1, -1);

        int[] featureBag = featureBagger.sample();

        for (long j = 0; j < groupSize; j++) {
            for (int i : featureBag) {
                var features = allFeatureVectors.get(group.get(j));

                var groupSizes = createSplit(i, features[i], group, groupSize, childGroups);

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
            bestChildGroups,
            bestGroupSizes
        );

    }

    @ValueClass
    interface Split {
        int index();

        double value();

        Groups groups();

        GroupSizes sizes();
    }

    @ValueClass
    interface StackRecord<PREDICTION> {
        TreeNode<PREDICTION> node();

        Split split();

        int depth();
    }

}
