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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

public class SplitMeanSquaredError implements ImpurityCriterion {

    private final HugeDoubleArray targets;

    public SplitMeanSquaredError(HugeDoubleArray targets) {
        this.targets = targets;
    }

    public static MemoryRange memoryEstimation() {
        return MemoryRange.of(sizeOfInstance(SplitMeanSquaredError.class));
    }

    @Override
    public MSEImpurityData groupImpurity(HugeLongArray group, long startIdx, long size) {
        if (size <= 0) {
            return new MSEImpurityData(0, 0, 0, 0);
        }

        double sum = 0;
        double sumOfSquares = 0;
        for (long i = startIdx; i < size; i++) {
            double value = targets.get(group.get(i));
            sum += value;
            sumOfSquares += value * value;
        }

        double mean = sum / size;
        double mse = sumOfSquares / size - mean * mean;

        return new MSEImpurityData(mse, sumOfSquares, sum, size);
    }

    @Override
    public void incrementalImpurity(long featureVectorIdx, ImpurityData impurityData) {
        var mseImpurityData = (MSEImpurityData) impurityData;

        double value = targets.get(featureVectorIdx);

        double sum = mseImpurityData.sum() + value;
        double sumOfSquares = mseImpurityData.sumOfSquares + value * value;
        long groupSize = mseImpurityData.groupSize + 1;

        updateImpurityData(sum, sumOfSquares, groupSize, mseImpurityData);
    }

    @Override
    public void decrementalImpurity(long featureVectorIdx, ImpurityData impurityData) {
        var mseImpurityData = (MSEImpurityData) impurityData;

        double value = targets.get(featureVectorIdx);

        double sum = mseImpurityData.sum() - value;
        double sumOfSquares = mseImpurityData.sumOfSquares - value * value;
        long groupSize = mseImpurityData.groupSize - 1;

        updateImpurityData(sum, sumOfSquares, groupSize, mseImpurityData);
    }

    private static void updateImpurityData(double sum, double sumOfSquares, long groupSize, MSEImpurityData mseImpurityData) {
        double mean = sum / groupSize;
        double mse = sumOfSquares / groupSize - mean * mean;

        mseImpurityData.setImpurity(mse);
        mseImpurityData.setSum(sum);
        mseImpurityData.setSumOfSquares(sumOfSquares);
        mseImpurityData.setGroupSize(groupSize);
    }

    static class MSEImpurityData implements ImpurityCriterion.ImpurityData {

        private double impurity;
        private double sumOfSquares;
        private double sum;
        private long groupSize;

        MSEImpurityData(double impurity, double sumOfSquares, double sum, long groupSize) {
            this.impurity = impurity;
            this.sumOfSquares = sumOfSquares;
            this.sum = sum;
            this.groupSize = groupSize;
        }

        public static long memoryEstimation() {
            return sizeOfInstance(MSEImpurityData.class);
        }

        @Override
        public double impurity() {
            return impurity;
        }

        @Override
        public long groupSize() {
            return groupSize;
        }

        @Override
        public void copyTo(ImpurityData impurityData) {
            var mseImpurityData = (MSEImpurityData) impurityData;
            mseImpurityData.setImpurity(impurity());
            mseImpurityData.setSumOfSquares(sumOfSquares());
            mseImpurityData.setSum(sum());
            mseImpurityData.setGroupSize(groupSize());
        }

        public void setGroupSize(long groupSize) {
            this.groupSize = groupSize;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public void setSumOfSquares(double sumOfSquares) {
            this.sumOfSquares = sumOfSquares;
        }

        public double sum() {
            return sum;
        }

        public double sumOfSquares() {
            return sumOfSquares;
        }

        public void setImpurity(double impurity) {
            this.impurity = impurity;
        }
    }
}
