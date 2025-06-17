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
package org.neo4j.gds.allshortestpaths;

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.msbfs.MSBFSMemoryEstimation;

public final class AllShortestPathsMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final boolean hasRelationshipWeightProperty;

    public AllShortestPathsMemoryEstimateDefinition(boolean hasRelationshipWeightProperty) {
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        if (hasRelationshipWeightProperty) {
            return weightedMemoryEstimation();
        } else {
            return unweightedMemoryEstimation();
        }
    }

    private MemoryEstimation weightedMemoryEstimation() {
        return MemoryEstimations.builder(WeightedAllShortestPaths.class)
            .perThread("ShortestPathTask", shortestPathTaskMemoryEstimation())
            .build();
    }

    private MemoryEstimation unweightedMemoryEstimation() {
        return MemoryEstimations.builder(MSBFSAllShortestPaths.class)
            .add("MSBFS", MSBFSMemoryEstimation.MSBFSWithANPStrategy(0))
            .build();
    }

    private MemoryEstimation shortestPathTaskMemoryEstimation() {
        return MemoryEstimations.builder(WeightedAllShortestPaths.ShortestPathTask.class)
            .perNode("distance array", Estimate::sizeOfDoubleArray)
            .add("priority queue", IntPriorityQueue.memoryEstimation())
            .build();
    }
} 
