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
import org.neo4j.gds.core.utils.paged.HugeIterativeMergeSort;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.ml.models.Features;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfIntArray;

public abstract class DecisionTreeTrainer<LOSS extends DecisionTreeLoss, PREDICTION> {

    private final LOSS lossFunction;
    private final Features features;
    private final DecisionTreeTrainerConfig config;
    private final FeatureBagger featureBagger;

    DecisionTreeTrainer(
        Features features,
        DecisionTreeTrainerConfig config,
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
        long numberOfBaggedFeatures,
        long leafNodeSize
    ) {
        var predictorEstimation = estimateTree(maxDepth, numberOfTrainingSamples, minSplitSize, leafNodeSize);

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

    public static MemoryRange estimateTree(
        int maxDepth,
        long numberOfTrainingSamples,
        int minSplitSize,
        long leafNodeSize
    ) {
        long maxNumLeafNodes = (long) Math.ceil(
            Math.min(
                Math.pow(2.0, maxDepth),
                // The parent of any leaf node must have had at least minSplitSize samples.
                // The number of parents of leaves is therefore limited by numberOfTrainingSamples / minSplitSize.
                2 * Math.ceil((double) numberOfTrainingSamples / minSplitSize)
            )
        );
        return MemoryRange.of(sizeOfInstance(DecisionTreePredictor.class))
            // Minimum size of tree depends on class distribution.
            .add(MemoryRange.of(1, maxNumLeafNodes).times(leafNodeSize))
            .add(MemoryRange.of(0, maxNumLeafNodes - 1).times(TreeNode.splitMemoryEstimation()));
    }

    public DecisionTreePredictor<PREDICTION> train(ReadOnlyHugeLongArray trainSetIndices) {
        var stack = new ArrayDeque<StackRecord<PREDICTION>>();
        TreeNode<PREDICTION> root;

        {
            var mutableTrainSetIndices = HugeLongArray.newArray(trainSetIndices.size());
            mutableTrainSetIndices.setAll(trainSetIndices::get);
            var impurityData = lossFunction.groupImpurity(mutableTrainSetIndices, 0, mutableTrainSetIndices.size());
            root = splitAndPush(
                stack,
                ImmutableGroup.of(mutableTrainSetIndices, 0, mutableTrainSetIndices.size(), impurityData),
                1
            );
        }

        int maxDepth = config.maxDepth();
        int minSplitSize = config.minSplitSize();

        while (!stack.isEmpty()) {
            var record = stack.pop();
            var split = record.split();

            if (record.depth() >= maxDepth || split.groups().left().size() < minSplitSize) {
                record
                    .node()
                    .setLeftChild(new TreeNode<>(toTerminal(split.groups().left())));
            } else {
                record.node().setLeftChild(
                    splitAndPush(
                        stack,
                        split.groups().left(),
                        record.depth() + 1
                    )
                );
            }

            if (record.depth() >= maxDepth || split.groups().right().size() < minSplitSize) {
                record.node().setRightChild(new TreeNode<>(toTerminal(split.groups().right())));
            } else {
                record.node().setRightChild(
                    splitAndPush(
                        stack,
                        split.groups().right(),
                        record.depth() + 1
                    )
                );
            }
        }

        return new DecisionTreePredictor<>(root);
    }

    protected abstract PREDICTION toTerminal(Group group);

    private TreeNode<PREDICTION> splitAndPush(
        Deque<StackRecord<PREDICTION>> stack,
        Group group,
        int depth
    ) {
        assert group.size() > 0;
        assert depth >= 1;

        if (group.size() < config.minSplitSize()) {
            return new TreeNode<>(toTerminal(group));
        }

        var split = findBestSplit(group);
        if (split.groups().right().size() == 0) {
            return new TreeNode<>(toTerminal(split.groups().left()));
        } else if (split.groups().left().size() == 0) {
            return new TreeNode<>(toTerminal(split.groups().right()));
        }

        var node = new TreeNode<PREDICTION>(split.index(), split.value());
        stack.push(ImmutableStackRecord.of(node, split, depth));

        return node;
    }

    private Split findBestSplit(Group group) {
        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestLoss = Double.MAX_VALUE;
        var sortCache = HugeLongArray.newArray(group.size());

        var leftChildArray = HugeLongArray.newArray(group.size());
        var rightChildArray = HugeLongArray.newArray(group.size());
        var bestLeftChildArray = HugeLongArray.newArray(group.size());
        var bestRightChildArray = HugeLongArray.newArray(group.size());
        long bestLeftGroupSize = -1;

        var bestLeftImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        var bestRightImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        var bestLeftImpurityDataForIdx = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        var bestRightImpurityDataForIdx = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        var rightImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);

        rightChildArray.setAll(idx -> group.array().get(group.startIdx() + idx));
        rightChildArray.copyTo(bestRightChildArray, group.size());

        int[] featureBag = featureBagger.sample();

        for (int i : featureBag) {
            double bestLossForIdx = Double.MAX_VALUE;
            double bestValueForIdx = Double.MAX_VALUE;
            long bestLeftGroupSizeForIdx = -1;

            HugeIterativeMergeSort.sort(rightChildArray, (long l) -> features.get(l)[i], sortCache);

            var leftImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
            group.impurityData().copyTo(rightImpurityData);

            for (long j = 0; j < group.size() - 1; j++) {
                long splittingFeatureVectorIdx = rightChildArray.get(j);
                leftChildArray.set(j, splittingFeatureVectorIdx);

                lossFunction.incrementalImpurity(splittingFeatureVectorIdx, leftImpurityData);
                lossFunction.decrementalImpurity(splittingFeatureVectorIdx, rightImpurityData);
                double loss = lossFunction.loss(leftImpurityData, rightImpurityData);

                if (loss < bestLossForIdx) {
                    bestValueForIdx = features.get(splittingFeatureVectorIdx)[i];
                    bestLossForIdx = loss;
                    leftImpurityData.copyTo(bestLeftImpurityDataForIdx);
                    rightImpurityData.copyTo(bestRightImpurityDataForIdx);
                    bestLeftGroupSizeForIdx = j + 1;
                }
            }

            if (bestLossForIdx < bestLoss) {
                bestIdx = i;
                bestValue = bestValueForIdx;
                bestLoss = bestLossForIdx;
                bestLeftGroupSize = bestLeftGroupSizeForIdx;

                bestLeftImpurityDataForIdx.copyTo(bestLeftImpurityData);
                bestRightImpurityDataForIdx.copyTo(bestRightImpurityData);

                var tmpChildArray = bestRightChildArray;
                bestRightChildArray = rightChildArray;
                rightChildArray = tmpChildArray;

                tmpChildArray = bestLeftChildArray;
                bestLeftChildArray = leftChildArray;
                leftChildArray = tmpChildArray;
            }
        }

        return ImmutableSplit.of(
            bestIdx,
            bestValue,
            ImmutableGroups.of(
                ImmutableGroup.of(
                    bestLeftChildArray,
                    0,
                    bestLeftGroupSize,
                    bestLeftImpurityData
                ),
                ImmutableGroup.of(
                    bestRightChildArray,
                    bestLeftGroupSize,
                    bestRightChildArray.size() - bestLeftGroupSize,
                    bestRightImpurityData
                )
            )
        );

    }

    @ValueClass
    interface Split {
        int index();

        double value();

        Groups groups();
    }

    @ValueClass
    interface StackRecord<PREDICTION> {
        TreeNode<PREDICTION> node();

        Split split();

        int depth();
    }
}
