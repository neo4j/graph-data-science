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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.Arrays;

public record DoubleAABB(double[] min, double[] max, int dimension)  implements  AABB{


    static DoubleAABB create(
        NodePropertyValues nodePropertyValues,
        HugeLongArray ids,
        long leftIndex,
        long rightIndex,
        int dimension
    ) {

        double[] min = new double[dimension];
        Arrays.fill(min, Double.MAX_VALUE);
        double[] max = new double[dimension];
        Arrays.fill(max, Double.MIN_VALUE);
        for (long i = leftIndex; i < rightIndex; i++) {
            var point = nodePropertyValues.doubleArrayValue(ids.get(i));
            for (int j = 0; j < dimension; j++) {
                min[j] = Math.min(min[j], point[j]);
                max[j] = Math.max(max[j], point[j]);
            }
        }
        return new DoubleAABB(min, max, dimension);

    }
    @Override
    public int mostSpreadDimension() {
        double bestSpread = max[0] - min[0];
        int index = 0;
        for (int i = 1; i < dimension; i++) {
            var spread = max[i] - min[i];
            if (spread > bestSpread) {
                index = i;
                bestSpread = spread;
            }
        }
        return index;
    }





    private double  lowerBoundForDimension(int dimension, double otherMin, double otherMax){
        var diff = Math.max(min[dimension], otherMin) - Math.min(max[dimension], otherMax);
        if (diff > 0) {
            return diff * diff;
        }
        return 0;
    }
}
