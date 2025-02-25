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

public class DoubleArrayDistances implements Distances {

    private final NodePropertyValues nodePropertyValues;

    public DoubleArrayDistances(NodePropertyValues nodePropertyValues) {
        this.nodePropertyValues = nodePropertyValues;
    }

    @Override
    public double computeDistanceUnsquared(long index1, long index2) {
        var array1 = nodePropertyValues.doubleArrayValue(index1);
        var array2 = nodePropertyValues.doubleArrayValue(index2);

        return Intersections.sumSquareDelta(array1,array2);
    }

    @Override
    public double lowerBound(AABB aabb, long index) {
        assert  aabb instanceof  DoubleAABB;
        var doubleAABB = (DoubleAABB) aabb;
        var min=doubleAABB.min();
        var max = doubleAABB.max();
        var dimension = doubleAABB.dimension();
        var lookupPoint = nodePropertyValues.doubleArrayValue(index);
        assert dimension == lookupPoint.length : "Lookup point has different dimension: " + lookupPoint.length + ". The box has dimension: " + dimension;
        double distance = 0d;
        for (int i = 0; i < dimension; i++) {
            distance += lowerBoundForDimension(min,max,i, lookupPoint[i],lookupPoint[i]);
        }
        return Math.sqrt(distance);
    }

    private double  lowerBoundForDimension(double[] min, double[] max,int dimension, double otherMin, double otherMax){

        var diff = Math.max(min[dimension], otherMin) - Math.min(max[dimension], otherMax);
        if (diff > 0) {
            return diff * diff;
        }
        return 0;
    }
}

