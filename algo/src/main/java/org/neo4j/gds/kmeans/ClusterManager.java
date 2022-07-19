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

import java.util.Arrays;
import java.util.List;

abstract class ClusterManager {

    final long[] nodesInCluster;
    final NodePropertyValues nodePropertyValues;
    final int dimensions;
    final int k;

    int currentlyAssigned;

    ClusterManager(NodePropertyValues values, int dimensions, int k) {
        this.dimensions = dimensions;
        this.k = k;
        this.nodePropertyValues = values;
        this.nodesInCluster = new long[k];
        this.currentlyAssigned = 0;
    }

    public int getCurrentlyAssigned() {
        return currentlyAssigned;
    }

    abstract void initialAssignCluster(long id);

    abstract void reset();

    abstract void normalizeClusters();

    abstract void updateFromTask(KmeansTask task);

    void initializeCentroids(List<Long> initialCentroidIds) {
        currentlyAssigned = 0;
        for (Long currentId : initialCentroidIds) {
            initialAssignCluster(currentId);
        }
    }
    
    abstract double[][] getCentroids();

    public long[] getNodesInCluster() {
        return nodesInCluster;
    }

    static ClusterManager createClusterManager(NodePropertyValues values, int dimensions, int k) {
        if (values.valueType() == ValueType.FLOAT_ARRAY) {
            return new FloatClusterManager(values, dimensions, k);
        }
        return new DoubleClusterManager(values, dimensions, k);
    }

    public abstract double euclidean(long nodeId, int centroidId);

    public int findClosestCentroid(long nodeId) {
        int community = 0;
        double smallestDistance = Double.MAX_VALUE;
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            double distance = euclidean(nodeId, centroidId);
            if (Double.compare(distance, smallestDistance) < 0) {
                smallestDistance = distance;
                community = centroidId;
            }
        }
        return community;
    }
}

class FloatClusterManager extends ClusterManager {
    private final float[][] centroids;

    FloatClusterManager(NodePropertyValues values, int dimensions, int k) {
        super(values, dimensions, k);
        this.centroids = new float[k][dimensions];
    }


    @Override
    public void reset() {
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            nodesInCluster[centroidId] = 0;
            Arrays.fill(centroids[centroidId], 0.0f);
        }
    }

    @Override
    public void normalizeClusters() {
        for (int centreId = 0; centreId < k; ++centreId) {
            for (int dimension = 0; dimension < dimensions; ++dimension)
                centroids[centreId][dimension] /= (float) nodesInCluster[centreId];
        }
    }

    @Override
    public void initialAssignCluster(long id) {
        float[] cluster = nodePropertyValues.floatArrayValue(id);
        System.arraycopy(cluster, 0, centroids[currentlyAssigned++], 0, cluster.length);
    }

    @Override
    public void updateFromTask(KmeansTask task) {
        var floatKmeansTask = (FloatKmeansTask) task;
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            nodesInCluster[centroidId] += task.getNumAssignedAtCluster(centroidId);
            var taskContributionToCluster = floatKmeansTask.getCentroidContribution(centroidId);
            for (int dimension = 0; dimension < dimensions; ++dimension) {
                centroids[centroidId][dimension] += taskContributionToCluster[dimension];
            }
        }
    }

    @Override
    double[][] getCentroids() {
        double[][] doubleCentroids = new double[k][dimensions];
        for (int i = 0; i < k; ++i) {
            for (int j = 0; j < dimensions; ++j) {
                doubleCentroids[i][j] = centroids[i][j];
            }
        }
        return doubleCentroids;
    }

    @Override
    public double euclidean(long nodeId, int centroidId) {
        float[] left = nodePropertyValues.floatArrayValue(nodeId);
        float[] right = centroids[centroidId];
        return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));
    }


}

class DoubleClusterManager extends ClusterManager {
    private final double[][] centroids;

    DoubleClusterManager(NodePropertyValues values, int dimensions, int k) {
        super(values, dimensions, k);
        this.centroids = new double[k][dimensions];
    }

    @Override
    public void reset() {
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            nodesInCluster[centroidId] = 0;
            Arrays.fill(centroids[centroidId], 0.0d);
        }
    }

    @Override
    public void normalizeClusters() {
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            for (int dimension = 0; dimension < dimensions; ++dimension)
                centroids[centroidId][dimension] /= (double) nodesInCluster[centroidId];
        }
    }

    @Override
    public void updateFromTask(KmeansTask task) {
        var doubleKmeansTask = (DoubleKmeansTask) task;
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            nodesInCluster[centroidId] += task.getNumAssignedAtCluster(centroidId);
            var taskContributionToCluster = doubleKmeansTask.getCentroidContribution(centroidId);
            for (int dimension = 0; dimension < dimensions; ++dimension) {
                centroids[centroidId][dimension] += taskContributionToCluster[dimension];
            }
        }
    }

    @Override
    public double euclidean(long nodeId, int centroidId) {
        double[] left = nodePropertyValues.doubleArrayValue(nodeId);
        double[] right = centroids[centroidId];
        return Math.sqrt(Intersections.sumSquareDelta(left, right, right.length));

    }

    @Override
    public void initialAssignCluster(long id) {
        double[] cluster = nodePropertyValues.doubleArrayValue(id);
        System.arraycopy(cluster, 0, centroids[currentlyAssigned++], 0, cluster.length);
    }

    @Override
    public double[][] getCentroids() {
        return centroids;
    }

}
