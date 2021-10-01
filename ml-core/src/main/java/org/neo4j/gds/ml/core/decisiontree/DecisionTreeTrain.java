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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.ThreadLocalRandom;

public abstract class DecisionTreeTrain<LOSS extends DecisionTreeLoss, PREDICTION> {

    private final Random random;
    private final AllocationTracker allocationTracker;
    private final LOSS lossFunction;
    private final HugeObjectArray<double[]> allFeatureVectors;
    private final int maxDepth;
    private final int minSize;
    private final int[] activeFeatureIndices;
    private final HugeLongArray activeFeatureVectors;

    DecisionTreeTrain(
        AllocationTracker allocationTracker,
        LOSS lossFunction,
        HugeObjectArray<double[]> allFeatureVectors,
        int maxDepth,
        int minSize,
        double numFeatureIndicesRatio,
        double numFeatureVectorsRatio,
        Optional<Random> random
    ) {
        assert allFeatureVectors.size() > 0;
        assert maxDepth >= 1;
        assert minSize >= 0;
        assert numFeatureIndicesRatio >= 0.0 && numFeatureIndicesRatio <= 1.0;
        assert numFeatureVectorsRatio >= 0.0 && numFeatureVectorsRatio <= 1.0;

        this.allocationTracker = allocationTracker;
        this.lossFunction = lossFunction;
        this.allFeatureVectors = allFeatureVectors;
        this.maxDepth = maxDepth;
        this.minSize = minSize;
        this.random = random.orElseGet(ThreadLocalRandom::current);
        this.activeFeatureIndices = sampleFeatureIndices(numFeatureIndicesRatio);
        this.activeFeatureVectors = sampleFeatureVectors(numFeatureVectorsRatio);
    }

    public DecisionTreePredict<PREDICTION> train() {
        var stack = new Stack<StackRecord<PREDICTION>>();
        TreeNode<PREDICTION> root = splitAndPush(stack, activeFeatureVectors, activeFeatureVectors.size(), 1);

        while (!stack.empty()) {
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

    protected abstract PREDICTION toTerminal(HugeLongArray group, long groupSize);

    private TreeNode<PREDICTION> splitAndPush(
        Stack<StackRecord<PREDICTION>> stack,
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
            HugeLongArray.newArray(groupSize, allocationTracker),
            HugeLongArray.newArray(groupSize, allocationTracker)
        );
        var bestChildGroups = ImmutableGroups.of(
            HugeLongArray.newArray(groupSize, allocationTracker),
            HugeLongArray.newArray(groupSize, allocationTracker)
        );
        var bestGroupSizes = ImmutableGroupSizes.of(-1, -1);

        for (int i : activeFeatureIndices) {
            for (long j = 0; j < groupSize; j++) {
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

    private int[] sampleFeatureIndices(double numFeatureIndicesRatio) {
        assert numFeatureIndicesRatio >= 0.0 && numFeatureIndicesRatio <= 1.0;

        int totalNumIndices = allFeatureVectors.get(0).length;
        final int numIndices = (int) Math.ceil(numFeatureIndicesRatio * allFeatureVectors.get(0).length);
        final var chosenIndices = new int[numIndices];

        var tmpAvailableIndices = new Integer[totalNumIndices];
        Arrays.setAll(tmpAvailableIndices, i -> i);
        final var availableIndices = new LinkedList<>(Arrays.asList(tmpAvailableIndices));

        for (int i = 0; i < numIndices; i++) {
            int j = random.nextInt(availableIndices.size());
            chosenIndices[i] = (availableIndices.get(j));
            availableIndices.remove(j);
        }

        return chosenIndices;
    }

    private HugeLongArray sampleFeatureVectors(double numFeatureVectorsRatio) {
        assert numFeatureVectorsRatio >= 0.0 && numFeatureVectorsRatio <= 1.0;

        if (new Double(0.0D).equals(numFeatureVectorsRatio)) {
            var allVectors = HugeLongArray.newArray(allFeatureVectors.size(), allocationTracker);
            allVectors.setAll(i -> i);
            return allVectors;
        }

        final long totalNumVectors = allFeatureVectors.size();
        final long numVectors = (long) Math.ceil(numFeatureVectorsRatio * totalNumVectors);
        final var chosenVectors = HugeLongArray.newArray(numVectors, allocationTracker);

        for (long i = 0; i < numVectors; i++) {
            long j = randomNonNegativeLong(0, totalNumVectors);
            chosenVectors.set(i, j);
        }

        return chosenVectors;
    }

    // Handle that `Math.abs(Long.MIN_VALUE) == Long.MIN_VALUE`.
    // `min` is inclusive, and `max` is exclusive.
    private long randomNonNegativeLong(long min, long max) {
        assert min >= 0;
        assert max > min;

        long randomNum;
        do {
            randomNum = random.nextLong();
        } while (randomNum == Long.MIN_VALUE);

        return (Math.abs(randomNum) % (max - min)) + min;
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
