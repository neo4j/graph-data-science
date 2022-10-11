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
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;

abstract class ClusterManager {

    final long[] nodesInCluster;

    final boolean[] shouldReset;
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
        this.shouldReset = new boolean[k];
    }

    int getCurrentlyAssigned() {
        return currentlyAssigned;
    }

    abstract void initialAssignCluster(long id);

    void reset() {
        for (int centroidId = 0; centroidId < k; ++centroidId) {
            nodesInCluster[centroidId] = 0;
            shouldReset[centroidId] = true;
        }
    }


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

    static MemoryEstimation memoryEstimation(int k, int fakeDimensions) {
        var builder = MemoryEstimations.builder(ClusterManager.class);
        builder
            .fixed("nodesInCluster", MemoryUsage.sizeOfLongArray(k))
            .fixed("shouldReset", MemoryUsage.sizeOfArray(k, 1L))
            .add("centroidsSize", MemoryEstimations.of("centroidsSize", MemoryRange.of(
                MemoryUsage.sizeOfFloatArray(fakeDimensions),
                MemoryUsage.sizeOfDoubleArray(fakeDimensions)
            )));
        return builder.build();
    }

    public abstract void assignSeededCentroids(List<List<Double>> seededCentroids);
}

