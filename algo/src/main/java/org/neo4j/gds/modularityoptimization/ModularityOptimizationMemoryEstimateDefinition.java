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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.Estimate;

public class ModularityOptimizationMemoryEstimateDefinition implements MemoryEstimateDefinition {

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(ModularityOptimization.class)
            .perNode("currentCommunities", HugeLongArray::memoryEstimation)
            .perNode("nextCommunities", HugeLongArray::memoryEstimation)
            .perNode("cumulativeNodeWeights", HugeDoubleArray::memoryEstimation)
            .perNode("nodeCommunityInfluences", HugeDoubleArray::memoryEstimation)
            .perNode("communityWeights", HugeAtomicDoubleArray::memoryEstimation)
            .perNode("colorsUsed", Estimate::sizeOfBitset)
            .perNode("colors", HugeLongArray::memoryEstimation)
            .rangePerNode(
                "reversedSeedCommunityMapping", (nodeCount) ->
                    MemoryRange.of(0, HugeLongArray.memoryEstimation(nodeCount))
            )
            .perNode("communityWeightUpdates", HugeAtomicDoubleArray::memoryEstimation)
            .perThread("ModularityOptimizationTask", MemoryEstimations.builder()
                .rangePerNode(
                    "communityInfluences",
                    (nodeCount) -> MemoryRange.of(
                        Estimate.sizeOfLongDoubleHashMap(50),
                        Estimate.sizeOfLongDoubleHashMap(Math.max(50, nodeCount))
                    )
                )
                .build()
            )
            .build();
    }
}
