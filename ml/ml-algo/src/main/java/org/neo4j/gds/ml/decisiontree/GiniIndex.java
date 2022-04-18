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
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

public class GiniIndex implements DecisionTreeLoss {

    private final HugeIntArray expectedMappedLabels;
    private final int numberOfClasses;

    public GiniIndex(HugeIntArray expectedMappedLabels, int numberOfClasses) {
        this.expectedMappedLabels = expectedMappedLabels;
        this.numberOfClasses = numberOfClasses;
    }

    public static GiniIndex fromOriginalLabels(
        HugeLongArray expectedOriginalLabels, LocalIdMap classMapping
    ) {
        assert expectedOriginalLabels.size() > 0;

        var mappedLabels = HugeIntArray.newArray(expectedOriginalLabels.size());
        mappedLabels.setAll(idx -> classMapping.toMapped(expectedOriginalLabels.get(idx)));

        return new GiniIndex(mappedLabels, classMapping.size());
    }

    public static MemoryRange memoryEstimation(long numberOfTrainingSamples) {
        return MemoryRange
            .of(HugeIntArray.memoryEstimation(numberOfTrainingSamples))
            .add(MemoryRange.of(sizeOfInstance(GiniIndex.class)));
    }

    @Override
    public GiniImpurityData groupImpurity(final HugeLongArray group, long startIndex, long size) {
        if (size == 0) {
            return new GiniImpurityData(0, new long[numberOfClasses], size);
        }

        final var groupClassCounts = new long[numberOfClasses];
        for (long i = startIndex; i < size; i++) {
            int expectedLabel = expectedMappedLabels.get(group.get(i));
            groupClassCounts[expectedLabel]++;
        }

        long sumOfSquares = 0;
        for (var count : groupClassCounts) {
            sumOfSquares += count * count;
        }
        double impurity = 1.0 - (double) sumOfSquares / (size * size);

        return new GiniImpurityData(impurity, groupClassCounts, size);
    }

    @Override
    public void incrementalImpurity(long featureVectorIdx, ImpurityData impurityData) {
        var giniImpurityData = (GiniImpurityData) impurityData;

        int label = expectedMappedLabels.get(featureVectorIdx);
        long newClassCount = giniImpurityData.classCounts[label] + 1;
        long newGroupSize = giniImpurityData.groupSize() + 1;

        updateImpurityData(label, newGroupSize, newClassCount, giniImpurityData);
    }

    @Override
    public void decrementalImpurity(long featureVectorIdx, ImpurityData impurityData) {
        var giniImpurityData = (GiniImpurityData) impurityData;

        int label = expectedMappedLabels.get(featureVectorIdx);
        long newClassCount = giniImpurityData.classCounts[label] - 1;
        long newGroupSize = giniImpurityData.groupSize() - 1;

        updateImpurityData(label, newGroupSize, newClassCount, giniImpurityData);
    }

    private static void updateImpurityData(int label, long newGroupSize, long newClassCount, GiniImpurityData impurityData) {
        long groupSizeSquared = impurityData.groupSize() * impurityData.groupSize();
        long newGroupSizeSquared = newGroupSize * newGroupSize;
        long prevClassCount = impurityData.classCounts()[label];

        double newLoss = impurityData.impurity();
        newLoss *= (double) groupSizeSquared / newGroupSizeSquared;
        newLoss += 1.0 - ((double) groupSizeSquared / newGroupSizeSquared);
        newLoss += (double) (prevClassCount * prevClassCount) / newGroupSizeSquared;
        newLoss -= (double) (newClassCount * newClassCount) / newGroupSizeSquared;

        impurityData.classCounts()[label] = newClassCount;
        impurityData.setGroupSize(newGroupSize);
        impurityData.setImpurity(newLoss);
    }

    static class GiniImpurityData implements ImpurityData {
        private double impurity;
        private final long[] classCounts;
        private long groupSize;

        GiniImpurityData(double impurity, long[] classCounts, long groupSize) {
            this.impurity = impurity;
            this.classCounts = classCounts;
            this.groupSize = groupSize;
        }

        @Override
        public void copyTo(ImpurityData impurityData) {
            var giniImpurityData = (GiniImpurityData) impurityData;
            giniImpurityData.setImpurity(impurity());
            giniImpurityData.setGroupSize(groupSize());
            System.arraycopy(classCounts(), 0, giniImpurityData.classCounts(), 0, classCounts().length);
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
