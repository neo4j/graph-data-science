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

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryUsage;

public class KmeansMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<KmeansBaseConfig> {

    @Override
    public MemoryEstimation memoryEstimation(KmeansBaseConfig configuration) {
        var fakeLength = 128;
        var builder = MemoryEstimations.builder(Kmeans.class)
            .perNode("bestCommunities", HugeIntArray::memoryEstimation)
            .fixed(
                "bestCentroids",
                MemoryUsage.sizeOfArray(configuration.k(), MemoryUsage.sizeOfDoubleArray(fakeLength))
            )
            .perNode("nodesInCluster", MemoryUsage::sizeOfLongArray)
            .perNode("distanceFromCentroid", HugeDoubleArray::memoryEstimation)
            .add(ClusterManager.memoryEstimation(
                configuration.k(),
                fakeLength
            ))
            .perThread("KMeansTask", KmeansTask.memoryEstimation(configuration.k(), fakeLength));

        if(configuration.computeSilhouette()) {
            builder.perNode("silhouette", HugeDoubleArray::memoryEstimation);
        }

        if(configuration.isSeeded()) {
            var centroids = configuration.seedCentroids();
            builder.fixed("seededCentroids", MemoryUsage.sizeOf(centroids));
        }

        return builder.build();
    }

}
