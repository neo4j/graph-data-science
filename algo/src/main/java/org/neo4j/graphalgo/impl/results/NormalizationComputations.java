/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.results;

import java.util.Arrays;
import java.util.stream.DoubleStream;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.parallelStream;

public class NormalizationComputations {
    public static double max(double[][] partitions) {
        double max = 1.0;
        for (double[] partition : partitions) {
            max = Math.max(max, NormalizationComputations.max(partition, max));
        }
        return max;
    }

    public static double l2Norm(double[][] partitions) {
        double sum = 0.0;
        for (double[] partition : partitions) {
            sum += squaredSum(partition);
        }
        return Math.sqrt(sum);
    }

    static double l1Norm(double[][] partitions) {
        double sum = 0.0;
        for (double[] partition : partitions) {
            sum += l1Norm(partition);
        }
        return sum;
    }

    static double squaredSum(double[] partition) {
        return parallelStream(Arrays.stream(partition), stream -> stream.map(value -> value * value).sum());
    }

    static double l1Norm(double[] partition) {
        return parallelStream(Arrays.stream(partition), DoubleStream::sum);
    }

    public static double max(double[] result, double defaultMax) {
        return parallelStream(Arrays.stream(result), stream -> stream.max().orElse(defaultMax));
    }
}
