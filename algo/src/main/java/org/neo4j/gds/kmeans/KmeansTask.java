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
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public interface KmeansTask extends Runnable {
    long getNumAssignedAtCenter(int ith);


    long getSwaps();

    static KmeansTask createTask(
        ClusterManager clusterManager,
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        int k,
        int dimensions,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        if (nodePropertyValues.valueType() == ValueType.DOUBLE_ARRAY) {
            return new DoubleKmeansTask(
                clusterManager,
                nodePropertyValues,
                communities,
                k,
                dimensions,
                partition,
                progressTracker
            );
        }
        return new FloatKmeansTask(
            clusterManager,
            nodePropertyValues,
            communities,
            k,
            dimensions,
            partition,
            progressTracker
        );
    }

}

final class DoubleKmeansTask implements KmeansTask {

    private final ProgressTracker progressTracker;
    private final Partition partition;
    private final double[][] communityCoordinateSums;
    private final NodePropertyValues nodePropertyValues;
    private final HugeIntArray communities;
    private final long[] communitySizes;
    private final ClusterManager clusterManager;
    private final int k;
    private final int dimensions;

    private long swaps;

    DoubleKmeansTask(
        ClusterManager clusterManager,
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        int k,
        int dimensions,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.progressTracker = progressTracker;
        this.partition = partition;
        this.clusterManager = clusterManager;
        this.communitySizes = new long[k];
        this.k = k;
        this.dimensions = dimensions;
        this.nodePropertyValues = nodePropertyValues;
        this.communities = communities;
        this.communityCoordinateSums = new double[k][dimensions];

    }

    double[] getCenterContribution(int ith) {
        return communityCoordinateSums[ith];
    }

    @Override
    public long getNumAssignedAtCenter(int ith) {
        return communitySizes[ith];
    }

    @Override
    public long getSwaps() {
        return swaps;
    }


    @Override
    public void run() {
        var startNode = partition.startNode();
        long endNode = startNode + partition.nodeCount();
        swaps = 0;

        for (int community = 0; community < k; ++community) {
            communitySizes[community] = 0;
            for (int dimension = 0; dimension < dimensions; ++dimension) {
                communityCoordinateSums[community][dimension] = 0.0;
            }
        }

        for (long nodeId = startNode; nodeId < endNode; nodeId++) {
            var property = nodePropertyValues.doubleArrayValue(nodeId);
            int community = clusterManager.findNextCenterForId(nodeId);
            communitySizes[community]++;
            int previousCommunity = communities.get(nodeId);
            if (community != previousCommunity) {
                swaps++;
            }
            //Note for potential improvement : This is potentially costly when clusters have somewhat stabilized.
            //Because we keep adding the same nodes to the same clusters. Perhaps instead of making the sum 0
            //we keep as is and do a subtraction when a node changes its cluster.
            //On that note,  maybe we can skip stable communities (i.e., communities that did not change between one iteration to another)
            // or avoid calculating their distance from other nodes etc...
            communities.set(nodeId, community);
            for (int j = 0; j < dimensions; ++j) {
                communityCoordinateSums[community][j] += property[j];
            }
        }

    }

}

final class FloatKmeansTask implements KmeansTask {

    private final ProgressTracker progressTracker;
    private final Partition partition;
    private final float[][] communityCoordinateSums;
    private final NodePropertyValues nodePropertyValues;
    private final HugeIntArray communities;
    private final long[] communitySizes;
    private final ClusterManager clusterManager;
    private final int k;
    private final int dimensions;

    private long swaps;

    FloatKmeansTask(
        ClusterManager clusterManager,
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        int k,
        int dimensions,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.progressTracker = progressTracker;
        this.partition = partition;
        this.clusterManager = clusterManager;
        this.communitySizes = new long[k];
        this.k = k;
        this.dimensions = dimensions;
        this.nodePropertyValues = nodePropertyValues;
        this.communities = communities;
        this.communityCoordinateSums = new float[k][dimensions];

    }

    float[] getCenterContribution(int ith) {
        return communityCoordinateSums[ith];
    }

    @Override
    public long getNumAssignedAtCenter(int ith) {
        return communitySizes[ith];
    }

    @Override
    public long getSwaps() {
        return swaps;
    }


    @Override
    public void run() {
        var startNode = partition.startNode();
        long endNode = startNode + partition.nodeCount();
        swaps = 0;

        for (int community = 0; community < k; ++community) {
            communitySizes[community] = 0;
            for (int dimension = 0; dimension < dimensions; ++dimension) {
                communityCoordinateSums[community][dimension] = 0.0f;
            }
        }

        for (long nodeId = startNode; nodeId < endNode; nodeId++) {
            var property = nodePropertyValues.floatArrayValue(nodeId);
            int community = clusterManager.findNextCenterForId(nodeId);
            communitySizes[community]++;
            int previousCommunity = communities.get(nodeId);
            if (community != previousCommunity) {
                swaps++;
            }
            communities.set(nodeId, community);
            for (int j = 0; j < dimensions; ++j) {
                communityCoordinateSums[community][j] += property[j];
            }
        }

    }

}

