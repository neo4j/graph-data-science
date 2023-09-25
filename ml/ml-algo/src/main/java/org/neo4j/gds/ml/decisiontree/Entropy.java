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

import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;

public class Entropy implements ImpurityCriterion {

    private static final double LN_2 = Math.log(2);

    private final HugeIntArray expectedMappedLabels;
    private final int numberOfClasses;

    public Entropy(HugeIntArray expectedMappedLabels, int numberOfClasses) {
        this.expectedMappedLabels = expectedMappedLabels;
        this.numberOfClasses = numberOfClasses;
    }

    public static MemoryRange memoryEstimation(long numberOfTrainingSamples) {
        return MemoryRange
            .of(HugeIntArray.memoryEstimation(numberOfTrainingSamples))
            .add(MemoryRange.of(sizeOfInstance(Entropy.class)));
    }

    @Override
    public EntropyImpurityData groupImpurity(final HugeLongArray group, long startIndex, long size) {
        if (size == 0) {
            return new EntropyImpurityData(0, new long[numberOfClasses], size);
        }

        final var groupClassCounts = new long[numberOfClasses];
        for (long i = startIndex; i < size; i++) {
            int expectedLabel = expectedMappedLabels.get(group.get(i));
            groupClassCounts[expectedLabel]++;
        }

        double impurity = 0;
        for (var count : groupClassCounts) {
            if (count == 0L) continue;

            double p = (double) count / size;
            impurity -= p * Math.log(p);
        }
        impurity /= LN_2;

        return new EntropyImpurityData(impurity, groupClassCounts, size);
    }

    @Override
    public void incrementalImpurity(long featureVectorIdx, ImpurityData impurityData) {
        var entropyImpurityData = (EntropyImpurityData) impurityData;

        int label = expectedMappedLabels.get(featureVectorIdx);
        long newClassCount = entropyImpurityData.classCounts[label] + 1;
        long newGroupSize = entropyImpurityData.groupSize() + 1;

        updateImpurityData(label, newGroupSize, newClassCount, entropyImpurityData);
    }

    @Override
    public void decrementalImpurity(long featureVectorIdx, ImpurityData impurityData) {
        var entropyImpurityData = (EntropyImpurityData) impurityData;

        int label = expectedMappedLabels.get(featureVectorIdx);
        long newClassCount = entropyImpurityData.classCounts[label] - 1;
        long newGroupSize = entropyImpurityData.groupSize() - 1;

        updateImpurityData(label, newGroupSize, newClassCount, entropyImpurityData);
    }

    private static void updateImpurityData(
        int label,
        long newGroupSize,
        long newClassCount,
        EntropyImpurityData impurityData
    ) {
        long prevClassCount = impurityData.classCounts()[label];

        double newImpurity = 0;
        if (newGroupSize > 0) {
            newImpurity = impurityData.impurity() * LN_2;
            if (impurityData.groupSize() > 0) {
                newImpurity -= Math.log(impurityData.groupSize());
                newImpurity *= impurityData.groupSize();
            }
            if (prevClassCount > 0) {
                newImpurity += prevClassCount * Math.log(prevClassCount);
            }
            if (newClassCount > 0) {
                newImpurity -= newClassCount * Math.log(newClassCount);
            }
            newImpurity /= newGroupSize;
            newImpurity += Math.log(newGroupSize);
            newImpurity /= LN_2;
        }

        impurityData.classCounts()[label] = newClassCount;
        impurityData.setGroupSize(newGroupSize);
        impurityData.setImpurity(newImpurity);
    }

    static class EntropyImpurityData implements ImpurityData {
        private double impurity;
        private final long[] classCounts;
        private long groupSize;

        EntropyImpurityData(double impurity, long[] classCounts, long groupSize) {
            this.impurity = impurity;
            this.classCounts = classCounts;
            this.groupSize = groupSize;
        }

        public static long memoryEstimation(int numberOfClasses) {
            return sizeOfInstance(EntropyImpurityData.class) + sizeOfLongArray(numberOfClasses);
        }

        @Override
        public void copyTo(ImpurityData impurityData) {
            var entropyImpurityData = (EntropyImpurityData) impurityData;
            entropyImpurityData.setImpurity(impurity());
            entropyImpurityData.setGroupSize(groupSize());
            System.arraycopy(classCounts(), 0, entropyImpurityData.classCounts(), 0, classCounts().length);
        }

        @Override
        public double impurity() {
            return impurity;
        }

        public void setImpurity(double impurity) {
            this.impurity = impurity;
        }

        public long[] classCounts() {
            return classCounts;
        }

        @Override
        public long groupSize() {
            return groupSize;
        }

        public void setGroupSize(long groupSize) {
            this.groupSize = groupSize;
        }
    }
}
