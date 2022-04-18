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

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeSerialIndirectMergeSort;
import org.neo4j.gds.ml.models.Features;

public class Splitter {

    private final DecisionTreeLoss lossFunction;
    private final Features features;
    private final FeatureBagger featureBagger;
    private final HugeLongArray sortCache;
    private final DecisionTreeLoss.ImpurityData bestLeftImpurityDataForIdx;
    private final DecisionTreeLoss.ImpurityData bestRightImpurityDataForIdx;
    private final DecisionTreeLoss.ImpurityData rightImpurityData;

    Splitter(long trainSetSize, DecisionTreeLoss lossFunction, FeatureBagger featureBagger, Features features) {
        this.featureBagger = featureBagger;
        this.lossFunction = lossFunction;
        this.features = features;
        this.sortCache = HugeLongArray.newArray(trainSetSize);
        this.bestLeftImpurityDataForIdx = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        this.bestRightImpurityDataForIdx = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        this.rightImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
    }

    DecisionTreeTrainer.Split findBestSplit(Group group) {
        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestLoss = Double.MAX_VALUE;
        long bestLeftGroupSize = -1;

        var leftChildArray = HugeLongArray.newArray(group.size());
        var rightChildArray = HugeLongArray.newArray(group.size());
        var bestLeftChildArray = HugeLongArray.newArray(group.size());
        var bestRightChildArray = HugeLongArray.newArray(group.size());

        var bestLeftImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
        var bestRightImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);

        rightChildArray.setAll(idx -> group.array().get(group.startIdx() + idx));
        rightChildArray.copyTo(bestRightChildArray, group.size());

        int[] featureBag = featureBagger.sample();

        for (int featureIdx : featureBag) {
            double bestLossForIdx = Double.MAX_VALUE;
            double bestValueForIdx = Double.MAX_VALUE;
            long bestLeftGroupSizeForIdx = -1;

            // By doing a sort of the group by this particular feature, all possible splits will simply be represented
            // by each index in the ordered group.
            HugeSerialIndirectMergeSort.sort(rightChildArray, group.size(), (long l) -> features.get(l)[featureIdx], sortCache);

            var leftImpurityData = lossFunction.groupImpurity(HugeLongArray.of(), 0, 0);
            group.impurityData().copyTo(rightImpurityData);

            for (long leftGroupSize = 1; leftGroupSize < group.size(); leftGroupSize++) {
                // At each step we move one feature vector to the left child from the right child. Since `rightChildArray` is
                // ordered by current feature, this simply entails copying the `leftGroupSize - 1`th entry from right to left.
                long splittingFeatureVectorIdx = rightChildArray.get(leftGroupSize - 1);
                leftChildArray.set(leftGroupSize - 1, splittingFeatureVectorIdx);

                // Since only one feature vector is moved to and from the left and right groups respectively, we can do an
                // impurity update based on the previous impurity.
                lossFunction.incrementalImpurity(splittingFeatureVectorIdx, leftImpurityData);
                lossFunction.decrementalImpurity(splittingFeatureVectorIdx, rightImpurityData);
                double loss = lossFunction.loss(leftImpurityData, rightImpurityData);

                // We track best split for a single feature idx in order to keep using `leftChildArray` and `rightChildArray`
                // throughout search for splits for this particular idx.
                if (loss < bestLossForIdx) {
                    bestValueForIdx = features.get(splittingFeatureVectorIdx)[featureIdx];
                    bestLossForIdx = loss;
                    leftImpurityData.copyTo(bestLeftImpurityDataForIdx);
                    rightImpurityData.copyTo(bestRightImpurityDataForIdx);
                    bestLeftGroupSizeForIdx = leftGroupSize;
                }
            }

            if (bestLossForIdx < bestLoss) {
                bestIdx = featureIdx;
                bestValue = bestValueForIdx;
                bestLoss = bestLossForIdx;
                bestLeftGroupSize = bestLeftGroupSizeForIdx;

                bestLeftImpurityDataForIdx.copyTo(bestLeftImpurityData);
                bestRightImpurityDataForIdx.copyTo(bestRightImpurityData);

                // At this time it's fine to swap array pointers since we will have to do a resort for the next feature
                // anyway.
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
                    group.size() - bestLeftGroupSize,
                    bestRightImpurityData
                )
            )
        );
    }
}
