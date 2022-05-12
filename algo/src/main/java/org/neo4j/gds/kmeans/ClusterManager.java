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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.Intersections;

import java.util.List;

interface ClusterManager {

    void initialAssignCluster(int i, long id);


    default void assignCenter(List<Long> initialCenterIds) {
        int clusterUpdateId = 0;
        for (Long currentId : initialCenterIds) {
            initialAssignCluster(clusterUpdateId++, currentId);
        }
    }

    int findNextCenterForId(long nodeId);

    static ClusterManager createClusterManager(NodePropertyValues values, int dimensions, int k) {
        if (values.valueType() == ValueType.FLOAT_ARRAY) {
            return new FloatClusterManager(values, dimensions, k);
        }
        return new DoubleClusterManager(values, dimensions, k);
    }
}

class FloatClusterManager implements ClusterManager {
    float[][] clusterCenters;
    NodePropertyValues nodePropertyValues;
    int dimensions;
    int k;

    public FloatClusterManager(NodePropertyValues values, int dimensions, int k) {
        this.dimensions = dimensions;
        this.k = k;
        nodePropertyValues = values;
    }

    @Override
    public void initialAssignCluster(int i, long id) {
        float[] cluster = nodePropertyValues.floatArrayValue(id);
        for (int dimension = 0; dimension < dimensions; ++dimension) {
            clusterCenters[i][dimension] = cluster[dimension];
        }
    }

    private float floatEuclidean(float[] left, float[] right) {
        return (float) Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));
    }

    @Override
    public int findNextCenterForId(long nodeId) {
        var property = nodePropertyValues.floatArrayValue(nodeId);
        int community = 0;
        float smallestDistance = Float.MAX_VALUE;
        for (int centerId = 0; centerId < k; ++centerId) {
            float distance = floatEuclidean(property, clusterCenters[centerId]);
            if (Float.compare(distance, smallestDistance) < 0) {
                smallestDistance = distance;
                community = centerId;
            }
        }
        return community;
    }
}

class DoubleClusterManager implements ClusterManager {
    double[][] clusterCenters;
    NodePropertyValues nodePropertyValues;
    int dimensions;
    int k;


    public DoubleClusterManager(NodePropertyValues values, int dimensions, int k) {
        this.dimensions = dimensions;
        this.k = k;
        nodePropertyValues = values;
    }

    @Override
    public void initialAssignCluster(int i, long id) {
        double[] cluster = nodePropertyValues.doubleArrayValue(id);
        for (int dimension = 0; dimension < dimensions; ++i) {
            clusterCenters[i][dimension] = cluster[dimension];
        }
    }

    private double doubleEuclidean(double[] left, double[] right) {
        return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));
    }

    @Override
    public int findNextCenterForId(long nodeId) {
        var property = nodePropertyValues.doubleArrayValue(nodeId);
        int community = 0;
        double smallestDistance = Double.MAX_VALUE;
        for (int centerId = 0; centerId < k; ++centerId) {
            double distance = doubleEuclidean(property, clusterCenters[centerId]);
            if (Double.compare(distance, smallestDistance) < 0) {
                smallestDistance = distance;
                community = centerId;
            }
        }
        return community;
    }

}
