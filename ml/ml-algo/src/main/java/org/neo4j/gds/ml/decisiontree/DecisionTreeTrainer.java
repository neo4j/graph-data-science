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

public abstract class DecisionTreeTrainer<PREDICTION extends Number> {

    private final ImpurityCriterion impurityCriterion;
    private final Features features;
    private final DecisionTreeTrainerConfig config;
    private final FeatureBagger featureBagger;
    private Splitter splitter;

    DecisionTreeTrainer(
        Features features,
        DecisionTreeTrainerConfig config,
        ImpurityCriterion impurityCriterion,
        FeatureBagger featureBagger
    ) {
        this.impurityCriterion = impurityCriterion;
        this.features = features;
        this.config = config;
        this.featureBagger = featureBagger;
    }

    // Does not include the class itself as it will be inherited anyway.
    public static MemoryRange estimateTree(
        DecisionTreeTrainerConfig config,
        long numberOfTrainingSamples,
        long leafNodeSizeInBytes,
        long sizeOfImpurityData
    ) {
        var predictorEstimation = estimateTree(
            config,
            numberOfTrainingSamples,
            leafNodeSizeInBytes
        );

        // The actual depth of the produced tree is capped by the number of samples that populate the leaves.
        long normalizedMaxDepth = Math.min(
            config.maxDepth(),
            Math.max(1, numberOfTrainingSamples - config.minSplitSize() + 2)
        );
        // Stack implies DFS, so will at most have 2 * normalizedMaxDepth entries for a binary tree.
        long maxItemsOnStack = 2L * normalizedMaxDepth;
        var maxStackSize = MemoryRange.of(sizeOfInstance(ArrayDeque.class))
            .add(MemoryRange.of(1, maxItemsOnStack).times(sizeOfInstance(ImmutableStackRecord.class)))
            .add(MemoryRange.of(
                0, // Only the input trainSet array ever resides in stack
                HugeLongArray.memoryEstimation(numberOfTrainingSamples / maxItemsOnStack) * maxItemsOnStack
            ));

        var splitterEstimation = Splitter.memoryEstimation(numberOfTrainingSamples, sizeOfImpurityData);

        return predictorEstimation
            .add(maxStackSize)
            .add(splitterEstimation);
    }

    public static MemoryRange estimateTree(
        DecisionTreeTrainerConfig config,
        long numberOfTrainingSamples,
        long leafNodeSizeInBytes
    ) {
        if (numberOfTrainingSamples == 0) {
            return MemoryRange.empty();
        }

        long maxNumLeafNodes = (long) Math.ceil(
            Math.min(
                Math.pow(2.0, config.maxDepth()),
                Math.min(
                    (double) numberOfTrainingSamples / config.minLeafSize(),
                    // The parent of any leaf node must have had at least minSplitSize samples.
                    // The number of parents of leaves is therefore limited by numberOfTrainingSamples / minSplitSize.
                    2.0 * numberOfTrainingSamples / config.minSplitSize()
                )
            )
        );
        return MemoryRange.of(sizeOfInstance(DecisionTreePredictor.class))
            // Minimum size of tree depends on class distribution.
            .add(MemoryRange.of(1, maxNumLeafNodes).times(leafNodeSizeInBytes))
            .add(MemoryRange.of(0, maxNumLeafNodes - 1).times(TreeNode.splitMemoryEstimation()));
    }

    public DecisionTreePredictor<PREDICTION> train(ReadOnlyHugeLongArray trainSetIndices) {
        splitter = new Splitter(
            trainSetIndices.size(),
            impurityCriterion,
            featureBagger,
            features,
            config.minLeafSize()
        );
        var stack = new ArrayDeque<StackRecord<PREDICTION>>();
        TreeNode<PREDICTION> root;

        // Use anonymous block to make `mutableTrainSetIndices` eligible for GC as soon as possible.
        {
            var mutableTrainSetIndices = HugeLongArray.newArray(trainSetIndices.size());
            mutableTrainSetIndices.setAll(trainSetIndices::get);
            var impurityData = impurityCriterion.groupImpurity(
                mutableTrainSetIndices,
                0,
                mutableTrainSetIndices.size()
            );
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

        var split = splitter.findBestSplit(group);
        if (split.groups().right().size() == 0) {
            return new TreeNode<>(toTerminal(split.groups().left()));
        } else if (split.groups().left().size() == 0) {
            return new TreeNode<>(toTerminal(split.groups().right()));
        }

        var node = new TreeNode<PREDICTION>(split.index(), split.value());
        stack.push(ImmutableStackRecord.of(node, split, depth));

        return node;
    }

    @ValueClass
    interface Split {
        int index();

        double value();

        Groups groups();
    }

    @ValueClass
    interface StackRecord<PREDICTION extends Number> {
        TreeNode<PREDICTION> node();

        Split split();

        int depth();
    }
}
