/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.modularity;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import static org.neo4j.graphalgo.core.ProcedureConstants.SEED_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_KEY;

public class ModularityOptimizationFactory extends AlgorithmFactory<ModularityOptimization, ProcedureConfiguration> {

    public static final int DEFAULT_MAX_ITERATIONS = 10;

    public static final MemoryEstimation MEMORY_ESTIMATION =
        MemoryEstimations.builder(ModularityOptimization.class)
            .perNode("currentCommunities", HugeLongArray::memoryEstimation)
            .perNode("nextCommunities", HugeLongArray::memoryEstimation)
            .perNode("cumulativeNodeWeights", HugeDoubleArray::memoryEstimation)
            .perNode("nodeCommunityInfluences", HugeDoubleArray::memoryEstimation)
            .perNode("communityWeights", HugeAtomicDoubleArray::memoryEstimation)
            .perNode("colorsUsed", MemoryUsage::sizeOfBitset)
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
                        MemoryUsage.sizeOfLongDoubleHashMap(50),
                        MemoryUsage.sizeOfLongDoubleHashMap(Math.max(50, nodeCount))
                    )
                )
                .build()
            )
            .build();

    @Override
    public MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    @Override
    public ModularityOptimization build(
        Graph graph, ProcedureConfiguration configuration, AllocationTracker tracker, Log log
    ) {
        NodeProperties seedProperty = configuration.getString(SEED_PROPERTY_KEY)
            .map(graph::nodeProperties)
            .orElse(null);

        return new ModularityOptimization(
            graph,
            configuration.getDirection(Direction.OUTGOING),
            configuration.getIterations(DEFAULT_MAX_ITERATIONS),
            configuration.getNumber(TOLERANCE_KEY, TOLERANCE_DEFAULT).doubleValue(),
            seedProperty,
            configuration.concurrency(),
            configuration.getBatchSize(),
            Pools.DEFAULT,
            tracker,
            log
        );
    }
}
