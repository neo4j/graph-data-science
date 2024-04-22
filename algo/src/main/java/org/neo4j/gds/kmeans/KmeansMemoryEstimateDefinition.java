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

import org.neo4j.gds.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.mem.Estimate;

import java.util.List;

public class KmeansMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final KmeansParameters parameters;

    public KmeansMemoryEstimateDefinition(KmeansParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var fakeLength = 128;
        var builder = MemoryEstimations.builder(Kmeans.class)
            .perNode("bestCommunities", HugeIntArray::memoryEstimation)
            .fixed(
                "bestCentroids",
                Estimate.sizeOfArray(parameters.k(), Estimate.sizeOfDoubleArray(fakeLength))
            )
            .perNode("nodesInCluster", Estimate::sizeOfLongArray)
            .perNode("distanceFromCentroid", HugeDoubleArray::memoryEstimation)
            .add(ClusterManager.memoryEstimation(
                parameters.k(),
                fakeLength
            ))
            .perThread("KMeansTask", KmeansTask.memoryEstimation(parameters.k(), fakeLength));

        if (parameters.computeSilhouette()) {
            builder.perNode("silhouette", HugeDoubleArray::memoryEstimation);
        }

        if (parameters.isSeeded()) {
            var sizeOfBoxedDouble = 8 + 8 + 8; // Double.BYTES + field + object
            var sizeOfList = Estimate.sizeOfInstance(List.class);
            var centroids = parameters.seedCentroids();
            var sizeOfCentroids = centroids.stream()
                .mapToLong(listOfDoubles -> sizeOfList + (long) listOfDoubles.size() * sizeOfBoxedDouble)
                .sum();
            builder.fixed("seededCentroids", sizeOfList + sizeOfCentroids);
        }

        return builder.build();
    }
}
