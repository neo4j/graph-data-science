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
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.Arrays;

final class FloatKmeansTask extends KmeansTask {

    private final float[][] communityCoordinateSums;

    FloatKmeansTask(
        KmeansSampler.SamplerType samplerType,
        ClusterManager clusterManager,
        NodePropertyValues nodePropertyValues,
        HugeIntArray communities,
        HugeDoubleArray distanceFromCluster,
        int k,
        int dimensions,
        Partition partition
    ) {
        super(
            samplerType,
            clusterManager,
            nodePropertyValues,
            communities,
            distanceFromCluster,
            k,
            dimensions,
            partition
        );
        this.communityCoordinateSums = new float[k][dimensions];
    }

    float[] getCentroidContribution(int ith) {
        return communityCoordinateSums[ith];
    }

    @Override
    void reset() {
        for (int community = 0; community < k; ++community) {
            communitySizes[community] = 0;
            Arrays.fill(communityCoordinateSums[community], 0.0f);
        }
    }

    @Override
    void updateAfterAssignmentToCentroid(long nodeId, int community) {
        var property = nodePropertyValues.floatArrayValue(nodeId);
        communities.set(nodeId, community);
        for (int j = 0; j < dimensions; ++j) {
            communityCoordinateSums[community][j] += property[j];
        }
    }


}
