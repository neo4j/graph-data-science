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
package org.neo4j.gds.ml.decisiontree;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.ml.models.Features;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;

public abstract class DecisionTreeTrain<LOSS extends DecisionTreeLoss, PREDICTION> {

    private final LOSS lossFunction;
    private final Features features;
    private final DecisionTreeTrainConfig config;
    private final FeatureBagger featureBagger;

    DecisionTreeTrain(
        Features features,
        DecisionTreeTrainConfig config,
        LOSS lossFunction,
        FeatureBagger featureBagger
    ) {
        this.lossFunction = lossFunction;
        this.features = features;
        this.config = config;
        this.featureBagger = featureBagger;
    }

    // Does not include the class itself as it will be inherited anyway.
    public static MemoryRange estimateTree(
        int maxDepth,
        int minSplitSize,
        long numberOfTrainingSamples,
        long numberOfBaggedFeatures
    ) {
        var predictorEstimation = estimateTree(maxDepth, numberOfTrainingSamples, minSplitSize);

        // The actual depth of the produced tree is capped by the number of samples to populate the leaves.
        long normalizedMaxDepth = Math.min(maxDepth, Math.max(1, numberOfTrainingSamples - minSplitSize + 2));
        // Stack implies DFS, so will at most have 2 * normalizedMaxDepth entries for a binary tree.
        long maxItemsOnStack = 2L * normalizedMaxDepth;
        var maxStackSize = MemoryRange.of(sizeOfInstance(ArrayDeque.class))
            .add(MemoryRange.of(1, maxItemsOnStack).times(sizeOfInstance(ImmutableStackRecord.class)))
            .add(MemoryRange.of(
                0, // Only the input trainSet array ever resides in stack
                HugeLongArray.memoryEstimation(numberOfTrainingSamples / maxItemsOnStack) * maxItemsOnStack
            ));

        var findBestSplitEstimation = MemoryRange.of(HugeLongArray.memoryEstimation(numberOfTrainingSamples) * 4)
            .add(sizeOfIntArray(numberOfBaggedFeatures));

        return predictorEstimation
            .add(maxStackSize)
            .add(findBestSplitEstimation);
    }

    public static MemoryRange estimateTree(int maxDepth, long numberOfTrainingSamples, int minSplitSize) {
        long maxNumLeafNodes = (long) Math.ceil(
            Math.min(
                Math.pow(2.0, maxDepth),
                // The parent of any leaf node must have had at least minSplitSize samples.
                // The number of parents of leaves is therefore limited by numberOfTrainingSamples / minSplitSize.
                2 * Math.ceil((double) numberOfTrainingSamples / minSplitSize)
            )
        );
        long maxNumNodes = 2 * maxNumLeafNodes - 1;
        return MemoryRange.of(sizeOfInstance(DecisionTreePredict.class))
            // Minimum size of tree depends on class distribution.
            .add(MemoryRange.of(1, maxNumNodes).times(TreeNode.memoryEstimation()));
    }

    public DecisionTreePredict<PREDICTION> train(ReadOnlyHugeLongArray trainSetIndices) {
        var stack = new ArrayDeque<StackRecord<PREDICTION>>();
        TreeNode<PREDICTION> root;

        root = splitAndPush(stack, trainSetIndices, trainSetIndices.size(), 1);

        int maxDepth = config.maxDepth();
        int minSplitSize = config.minSplitSize();

        while (!stack.isEmpty()) {
            var record = stack.pop();
            var split = record.split();

            if (record.depth() >= maxDepth || split.sizes().left() < minSplitSize) {
                record.node().setLeftChild(new TreeNode<>(toTerminal(split.groups().left(), split.sizes().left())));
            } else {
                record.node().setLeftChild(
                    splitAndPush(
                        stack,
                        split.groups().left(),
                        split.sizes().left(),
                        record.depth() + 1
                    )
                );
            }

            if (record.depth() >= maxDepth || split.sizes().right() < minSplitSize) {
                record.node().setRightChild(new TreeNode<>(toTerminal(split.groups().right(), split.sizes().right())));
            } else {
                record.node().setRightChild(
                    splitAndPush(
                        stack,
                        split.groups().right(),
                        split.sizes().right(),
                        record.depth() + 1
                    )
                );
            }
        }

        return new DecisionTreePredict<>(root);
    }

    protected abstract PREDICTION toTerminal(ReadOnlyHugeLongArray group, long groupSize);

    private TreeNode<PREDICTION> splitAndPush(
        Deque<StackRecord<PREDICTION>> stack,
        ReadOnlyHugeLongArray group,
        long groupSize,
        int depth
    ) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert depth >= 1;

        var split = findBestSplit(group, groupSize);
        if (split.sizes().right() == 0) {
            return new TreeNode<>(toTerminal(split.groups().left(), split.sizes().left()));
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
        ReadOnlyHugeLongArray group,
        final long groupSize,
        Groups groups
    ) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert index >= 0 && index < features.featureDimension();

        long leftGroupSize = 0;
        long rightGroupSize = 0;

        final var leftGroup = groups.left();
        final var rightGroup = groups.right();

        for (int i = 0; i < groupSize; i++) {
            var featuresIdx = group.get(i);
            double[] featureVector = features.get(featuresIdx);
            if (featureVector[index] < value) {
                leftGroup.set(leftGroupSize++, featuresIdx);
            } else {
                rightGroup.set(rightGroupSize++, featuresIdx);
            }
        }

        return ImmutableGroupSizes.of(leftGroupSize, rightGroupSize);
    }

    private Split findBestSplit(final ReadOnlyHugeLongArray group, final long groupSize) {
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
                double[] featureVector = features.get(group.get(j));

                var groupSizes = createSplit(i, featureVector[i], group, groupSize, childGroups);

                var loss = lossFunction.splitLoss(childGroups, groupSizes);

                if (loss < bestLoss) {
                    bestIdx = i;
                    bestValue = featureVector[i];
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
            ImmutableReadOnlyGroups.of(
                ReadOnlyHugeLongArray.of(bestChildGroups.left()),
                ReadOnlyHugeLongArray.of(bestChildGroups.right())
            ),
            bestGroupSizes
        );

    }

    @ValueClass
    interface Split {
        int index();

        double value();

        ReadOnlyGroups groups();

        GroupSizes sizes();
    }

    @ValueClass
    interface StackRecord<PREDICTION> {
        TreeNode<PREDICTION> node();

        Split split();

        int depth();
    }

}
