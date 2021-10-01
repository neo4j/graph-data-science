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
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.Optional;

public class ClassificationDecisionTreeTrain<L extends DecisionTreeLoss> extends DecisionTreeTrain<L, Integer> {

    private final int[] classes;
    private final HugeIntArray allLabels;

    public ClassificationDecisionTreeTrain(
        AllocationTracker allocationTracker,
        L lossFunction,
        HugeObjectArray<double[]> allFeatures,
        int maxDepth,
        int minSize,
        double numFeatureIndicesRatio,
        double numFeatureVectorsRatio,
        Optional<Long> randomSeed,
        int[] classes,
        HugeIntArray allLabels
    ) {
        super(
            allocationTracker,
            lossFunction,
            allFeatures,
            maxDepth,
            minSize,
            numFeatureIndicesRatio,
            numFeatureVectorsRatio,
            randomSeed
        );

        assert classes.length > 0;
        this.classes = classes;

        assert allLabels.size() == allFeatures.size();
        this.allLabels = allLabels;
    }

    public static final class Builder<L extends DecisionTreeLoss> {

        private final AllocationTracker allocationTracker;
        private final L lossFunction;
        private final HugeObjectArray<double[]> allFeatures;
        private final int maxDepth;
        private final int[] classes;
        private final HugeIntArray allLabels;

        private int minSize = 1;
        private double numFeatureIndicesRatio = 1.0;
        private double numFeatureVectorsRatio = 0.0; // Use all feature vectors.
        private Optional<Long> randomSeed = Optional.empty();

        public Builder(
            AllocationTracker allocationTracker,
            L lossFunction,
            HugeObjectArray<double[]> allFeatures,
            int maxDepth,
            int[] classes,
            HugeIntArray allLabels
        ) {
            this.allocationTracker = allocationTracker;
            this.lossFunction = lossFunction;
            this.allFeatures = allFeatures;
            this.maxDepth = maxDepth;
            this.classes = classes;
            this.allLabels = allLabels;
        }

        public ClassificationDecisionTreeTrain<L> build() {
            return new ClassificationDecisionTreeTrain<>(
                allocationTracker,
                lossFunction,
                allFeatures,
                maxDepth,
                minSize,
                numFeatureIndicesRatio,
                numFeatureVectorsRatio,
                randomSeed,
                classes,
                allLabels
            );
        }

        public Builder<L> withMinSize(int minSize) {
            this.minSize = minSize;
            return this;
        }

        public Builder<L> withNumFeatureIndicesRatio(double ratio) {
            this.numFeatureIndicesRatio = ratio;
            return this;
        }

        public Builder<L> withNumFeatureVectorsRatio(double ratio) {
            this.numFeatureVectorsRatio = ratio;
            return this;
        }

        public Builder<L> withRandomSeed(long seed) {
            this.randomSeed = Optional.of(seed);
            return this;
        }
    }

    @Override
    protected Integer toTerminal(final HugeLongArray group, final long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        final var classesInGroup = new long[classes.length];

        for (long i = 0; i < groupSize; i++) {
            classesInGroup[allLabels.get(group.get(i))]++;
        }

        long max = -1;
        int maxClass = 0;
        for (int i = 0; i < classesInGroup.length; i++) {
            if (classesInGroup[i] <= max) continue;

            max = classesInGroup[i];
            maxClass = i;
        }

        return maxClass;
    }
}
