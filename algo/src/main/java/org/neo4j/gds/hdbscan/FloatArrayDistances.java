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
import org.neo4j.gds.core.utils.Intersections;

public class FloatArrayDistances implements Distances {

    private final NodePropertyValues nodePropertyValues;

    public FloatArrayDistances(NodePropertyValues nodePropertyValues) {
        this.nodePropertyValues = nodePropertyValues;
    }

    @Override
    public double computeDistanceUnsquared(long index1, long index2) {
        var array1 = nodePropertyValues.floatArrayValue(index1);
        var array2 = nodePropertyValues.floatArrayValue(index2);

        return Intersections.sumSquareDelta(array1,array2);
    }

    @Override
    public double lowerBound(AABB aabb, long index) {
        var min = aabb.min();
        var max = aabb.max();
        var dimension = aabb.dimension();
        var lookupPoint = nodePropertyValues.floatArrayValue(index);
        assert dimension == lookupPoint.length : "Lookup point has different dimension: " + lookupPoint.length + ". The box has dimension: " + dimension;
        double distance = 0d;
        for (int i = 0; i < dimension; i++) {
            distance += lowerBoundForDimension(min,max,i, lookupPoint[i],lookupPoint[i]);
        }
        return Math.sqrt(distance);
    }

    private double  lowerBoundForDimension(double[] min, double[] max,int dimension, float otherMin, float otherMax){

        double diff = Math.max(min[dimension], otherMin) - Math.min(max[dimension], otherMax);
        if (diff > 0) {
            return diff * diff;
        }
        return 0;
    }
}

