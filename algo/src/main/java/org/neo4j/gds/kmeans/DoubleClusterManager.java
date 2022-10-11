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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.Intersections;

import java.util.Arrays;
import java.util.List;

class DoubleClusterManager extends ClusterManager {
    private final double[][] centroids;

    DoubleClusterManager(NodePropertyValues values, int dimensions, int k) {
        super(values, dimensions, k);
        this.centroids = new double[k][dimensions];
    }


    @Override
    public void normalizeClusters() {
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            for (int dimension = 0; dimension < dimensions; ++dimension) {
                if (nodesInCluster[centroidId] > 0) {
                    centroids[centroidId][dimension] /= (double) nodesInCluster[centroidId];
                }
            }
        }
    }

    @Override
    public void updateFromTask(KmeansTask task) {
        var doubleKmeansTask = (DoubleKmeansTask) task;
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            var contribution = task.getNumAssignedAtCluster(centroidId);
            if (contribution > 0) {
                if (shouldReset[centroidId]) {
                    Arrays.fill(centroids[centroidId], 0.0d);
                    shouldReset[centroidId] = false;
                }
                nodesInCluster[centroidId] += contribution;
                var taskContributionToCluster = doubleKmeansTask.getCentroidContribution(centroidId);
                for (int dimension = 0; dimension < dimensions; ++dimension) {
                    centroids[centroidId][dimension] += taskContributionToCluster[dimension];
                }
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

    @Override
    public void assignSeededCentroids(List<List<Double>> seededCentroids) {

        for (List<Double> centroid : seededCentroids) {
            var centroidArray = new double[dimensions];
            int index = 0;
            for (double value : centroid) {
                centroidArray[index++] = value;
            }
            System.arraycopy(centroidArray, 0, centroids[currentlyAssigned++], 0, centroidArray.length);
        }
    }

}
