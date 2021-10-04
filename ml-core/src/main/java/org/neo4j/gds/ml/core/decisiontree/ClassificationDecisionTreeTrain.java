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

import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class ClassificationDecisionTreeTrain<LOSS extends DecisionTreeLoss> extends DecisionTreeTrain<LOSS, Integer> {

    private final int[] classes;
    private final HugeIntArray allLabels;
    private final Map<Integer, Integer> classToIdx;

    public ClassificationDecisionTreeTrain(
        AllocationTracker allocationTracker,
        LOSS lossFunction,
        HugeObjectArray<double[]> allFeatures,
        int maxDepth,
        int minSize,
        double featureBaggingRatio,
        double numFeatureVectorsRatio,
        Optional<Random> random,
        int[] classes,
        HugeIntArray allLabels,
        Map<Integer, Integer> classToIdx
    ) {
        super(
            allocationTracker,
            lossFunction,
            allFeatures,
            maxDepth,
            minSize,
            featureBaggingRatio,
            numFeatureVectorsRatio,
            random
        );

        assert classes.length > 0;
        this.classes = classes;

        assert allLabels.size() == allFeatures.size();
        this.allLabels = allLabels;

        assert classToIdx.keySet().size() == classes.length;
        this.classToIdx = classToIdx;
    }

    public static final class Builder<LOSS extends DecisionTreeLoss> {

        private final AllocationTracker allocationTracker;
        private final LOSS lossFunction;
        private final HugeObjectArray<double[]> allFeatures;
        private final int maxDepth;
        private final int[] classes;
        private final HugeIntArray allLabels;
        private final Map<Integer, Integer> classToIdx;

        private int minSize = 1;
        private double featureBaggingRatio = 0.0; // Use all feature indices.
        private double numFeatureVectorsRatio = 0.0; // Use all feature vectors.
        private Optional<Random> random = Optional.empty();

        public Builder(
            AllocationTracker allocationTracker,
            LOSS lossFunction,
            HugeObjectArray<double[]> allFeatures,
            int maxDepth,
            int[] classes,
            HugeIntArray allLabels,
            Map<Integer, Integer> classToIdx
        ) {
            this.allocationTracker = allocationTracker;
            this.lossFunction = lossFunction;
            this.allFeatures = allFeatures;
            this.maxDepth = maxDepth;
            this.classes = classes;
            this.allLabels = allLabels;
            this.classToIdx = classToIdx;
        }

        public ClassificationDecisionTreeTrain<LOSS> build() {
            return new ClassificationDecisionTreeTrain<>(
                allocationTracker,
                lossFunction,
                allFeatures,
                maxDepth,
                minSize,
                featureBaggingRatio,
                numFeatureVectorsRatio,
                random,
                classes,
                allLabels,
                classToIdx
            );
        }

        public Builder<LOSS> withMinSize(int minSize) {
            this.minSize = minSize;
            return this;
        }

        public Builder<LOSS> withFeatureBaggingRatio(double ratio) {
            this.featureBaggingRatio = ratio;
            return this;
        }

        public Builder<LOSS> withNumFeatureVectorsRatio(double ratio) {
            this.numFeatureVectorsRatio = ratio;
            return this;
        }

        public Builder<LOSS> withRandomSeed(long seed) {
            this.random = Optional.of(new Random(seed));
            return this;
        }
    }

    @Override
    protected Integer toTerminal(final HugeLongArray group, final long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        final var classesInGroup = new long[classes.length];

        for (long i = 0; i < groupSize; i++) {
            int c = allLabels.get(group.get(i));
            classesInGroup[classToIdx.get(c)]++;
        }

        long max = -1;
        int maxClassIdx = 0;
        for (int i = 0; i < classesInGroup.length; i++) {
            if (classesInGroup[i] <= max) continue;

            max = classesInGroup[i];
            maxClassIdx = i;
        }

        return classes[maxClassIdx];
    }
}
