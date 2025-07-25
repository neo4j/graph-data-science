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
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

import java.util.List;

final class ClusterManager {

    private final long[] nodesInCluster;
    private final Coordinates centroids;
    private final boolean[] shouldReset;
    private final int dimensions;
    private final int k;

    private int currentlyAssigned;

   private ClusterManager(Coordinates centroids, NodePropertyValues values, int dimensions, int k) {
        this.dimensions = dimensions;
        this.k = k;
       this.nodesInCluster = new long[k];
        this.currentlyAssigned = 0;
        this.shouldReset = new boolean[k];
        this.centroids = centroids;
    }
    static ClusterManager create( NodePropertyValues values, int dimensions, int k, CoordinatesSupplier coordinatesSupplier){
       var centroids = coordinatesSupplier.get();
       return new ClusterManager(
           centroids,
           values,
           dimensions
           ,k
       );
    }


    int getCurrentlyAssigned() {
        return currentlyAssigned;
    }

    void initialAssignCluster(long nodeId){
            centroids.assignTo(nodeId,currentlyAssigned++);
   }

    void reset() {
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            nodesInCluster[centroidId] = 0;
            shouldReset[centroidId] = true;
        }
    }

     void normalizeClusters(){
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            if (nodesInCluster[centroidId] > 0) {
                for (int dimension = 0; dimension < dimensions; ++dimension) {
                    normalize(centroidId,dimension);
                }
            }
        }
    }
    void normalize(int centroidId,int dimension){
        centroids.normalizeDimension(centroidId,dimension,nodesInCluster[centroidId]);
    }

    void  updateFromTask(KmeansTask task){
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            var contribution = task.getNumAssignedAtCluster(centroidId);
            if (contribution > 0) {
                if (shouldReset[centroidId]) {
                    centroids.reset(centroidId);
                    shouldReset[centroidId] = false;
                }
                nodesInCluster[centroidId] += contribution;
                centroids.add(centroidId,task.clusterContributions());
            }
        }
    }

    void initializeCentroids(List<Long> initialCentroidIds) {
        for (Long currentId : initialCentroidIds) {
            initialAssignCluster(currentId);
        }
    }

     double[][] getCentroids(){
        return  centroids.coordinates();
     }

     long[] getNodesInCluster() {
        return nodesInCluster;
    }

    int findClosestCentroid(long nodeId) {
        int community = 0;
        double smallestDistance = Double.MAX_VALUE;
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            double distance = centroids.euclideanDistance(nodeId, centroidId);
            if (Double.compare(distance, smallestDistance) < 0) {
                smallestDistance = distance;
                community = centroidId;
            }
        }
        return community;
    }

    double euclidean(long nodeId, int centroidId){
       return  centroids.euclideanDistance(nodeId,centroidId);
    }

    static MemoryEstimation memoryEstimation(int k, int fakeDimensions) {
        var builder = MemoryEstimations.builder(ClusterManager.class);
        builder
            .fixed("nodesInCluster", Estimate.sizeOfLongArray(k))
            .fixed("shouldReset", Estimate.sizeOfArray(k, 1L))
            .add("centroidsSize", MemoryEstimations.of("centroidsSize", MemoryRange.of(
                Estimate.sizeOfFloatArray(fakeDimensions),
                Estimate.sizeOfDoubleArray(fakeDimensions)
            )));
        return builder.build();
    }

    void assignSeededCentroids(List<List<Double>> seededCentroids){
       centroids.assign(seededCentroids);
       currentlyAssigned+=seededCentroids.size();
    }
}
