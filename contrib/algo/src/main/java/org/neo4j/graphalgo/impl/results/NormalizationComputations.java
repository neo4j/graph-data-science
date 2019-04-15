/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
        return Arrays.stream(partition).parallel().map(value -> value * value).sum();
    }

    static double l1Norm(double[] partition) {
        return Arrays.stream(partition).parallel().sum();
    }

    public static double max(double[] result, double defaultMax) {
        return Arrays.stream(result).parallel().max().orElse(defaultMax);
    }
}
