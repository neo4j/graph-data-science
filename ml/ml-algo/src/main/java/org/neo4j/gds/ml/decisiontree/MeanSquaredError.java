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
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

public class MeanSquaredError implements DecisionTreeLoss {

    private final HugeDoubleArray targets;

    public MeanSquaredError(HugeDoubleArray targets) {
        this.targets = targets;
    }

    public static MemoryRange memoryEstimation() {
        return MemoryRange.of(sizeOfInstance(MeanSquaredError.class));
    }

    @Override
    public double splitLoss(Groups groups, GroupSizes groupSizes) {
        double leftMean = groupMean(groups.left(), groupSizes.left());
        double rightMean = groupMean(groups.right(), groupSizes.right());

        double leftMeanSquaredError = groupMeanSquaredError(groups.left(), groupSizes.left(), leftMean);
        double rightMeanSquaredError = groupMeanSquaredError(groups.right(), groupSizes.right(), rightMean);

        return leftMeanSquaredError + rightMeanSquaredError;
    }

    private double groupMean(HugeLongArray group, long size) {
        if (size == 0) return 0;

        double sum = 0;
        for (long i = 0; i < size; i++) {
            sum += targets.get(group.get(i));
        }

        return sum / size;
    }

    private double groupMeanSquaredError(HugeLongArray group, long size, double mean) {
        if (size == 0) return 0;

        double squaredError = 0;
        for (long i = 0; i < size; i++) {
            double error = mean - targets.get(group.get(i));
            squaredError += error * error;
        }

        return squaredError / size;
    }
}
