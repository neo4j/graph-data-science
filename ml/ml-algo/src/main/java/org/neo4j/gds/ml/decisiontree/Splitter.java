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

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeSerialIndirectMergeSort;
import org.neo4j.gds.ml.models.Features;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

public class Splitter {

    private final ImpurityCriterion impurityCriterion;
    private final Features features;
    private final FeatureBagger featureBagger;
    private final int minLeafSize;
    private final HugeLongArray sortCache;
    private final ImpurityCriterion.ImpurityData rightImpurityData;

    Splitter(long trainSetSize, ImpurityCriterion impurityCriterion, FeatureBagger featureBagger, Features features, int minLeafSize) {
        this.featureBagger = featureBagger;
        this.impurityCriterion = impurityCriterion;
        this.features = features;
        this.minLeafSize = minLeafSize;
        this.sortCache = HugeLongArray.newArray(trainSetSize);
        this.rightImpurityData = impurityCriterion.groupImpurity(HugeLongArray.of(), 0, 0);
    }

    static long memoryEstimation(long numberOfTrainingSamples, long sizeOfImpurityData) {
        return sizeOfInstance(Splitter.class)
               // sort cache
               + HugeLongArray.memoryEstimation(numberOfTrainingSamples)
               // impurity data cache
               + 4 * sizeOfImpurityData
               // group cache
               + 4 * HugeLongArray.memoryEstimation(numberOfTrainingSamples);
    }

    DecisionTreeTrainer.Split findBestSplit(Group group) {
        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestImpurity = Double.MAX_VALUE;
        long bestLeftGroupSize = -1;

        var leftChildArray = HugeLongArray.newArray(group.size());
        var rightChildArray = HugeLongArray.newArray(group.size());
        var bestLeftChildArray = HugeLongArray.newArray(group.size());
        var bestRightChildArray = HugeLongArray.newArray(group.size());

        var bestLeftImpurityData = impurityCriterion.groupImpurity(HugeLongArray.of(), 0, 0);
        var bestRightImpurityData = impurityCriterion.groupImpurity(HugeLongArray.of(), 0, 0);

        rightChildArray.setAll(idx -> group.array().get(group.startIdx() + idx));
        rightChildArray.copyTo(bestRightChildArray, group.size());

        int[] featureBag = featureBagger.sample();

        for (int featureIdx : featureBag) {
            // By doing a sort of the group by this particular feature, all possible splits will simply be represented
            // by each index in the ordered group.
            HugeSerialIndirectMergeSort.sort(rightChildArray, group.size(), (long l) -> features.get(l)[featureIdx], sortCache);

            group.impurityData().copyTo(rightImpurityData);

            for (long leftGroupSize = 1; leftGroupSize < minLeafSize; leftGroupSize++) {
                // At each step we move one feature vector to the left child from the right child. Since `rightChildArray` is
                // ordered by current feature, this simply entails copying the `leftGroupSize - 1`th entry from right to left.
                long splittingFeatureVectorIdx = rightChildArray.get(leftGroupSize - 1);
                leftChildArray.set(leftGroupSize - 1, splittingFeatureVectorIdx);

                // Since only one feature vector is moved to the right group, we can do an
                // impurity update based on the previous impurity.
                impurityCriterion.decrementalImpurity(splittingFeatureVectorIdx, rightImpurityData);
            }

            var leftImpurityData = impurityCriterion.groupImpurity(leftChildArray, 0, minLeafSize - 1L);
            boolean foundImprovementWithIdx = false;

            // Continue moving feature vectors, but now actually compute combined impurity since left group is large enough.
            for (long leftGroupSize = minLeafSize; leftGroupSize <= group.size() - minLeafSize; leftGroupSize++) {
                long splittingFeatureVectorIdx = rightChildArray.get(leftGroupSize - 1);
                leftChildArray.set(leftGroupSize - 1, splittingFeatureVectorIdx);

                impurityCriterion.incrementalImpurity(splittingFeatureVectorIdx, leftImpurityData);
                impurityCriterion.decrementalImpurity(splittingFeatureVectorIdx, rightImpurityData);

                double combinedImpurity = impurityCriterion.combinedImpurity(leftImpurityData, rightImpurityData);

                // We track best split for a single feature idx in order to keep using `leftChildArray` and `rightChildArray`
                // throughout search for splits for this particular idx.
                if (combinedImpurity < bestImpurity) {
                    foundImprovementWithIdx = true;
                    bestIdx = featureIdx;
                    bestValue = features.get(splittingFeatureVectorIdx)[featureIdx];
                    bestImpurity = combinedImpurity;
                    bestLeftGroupSize = leftGroupSize;
                    leftImpurityData.copyTo(bestLeftImpurityData);
                    rightImpurityData.copyTo(bestRightImpurityData);
                }
            }

            if (foundImprovementWithIdx) {
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
