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
package org.neo4j.graphalgo.impl.louvain.legacy;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.logging.Log;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

public class LouvainFactory extends AlgorithmFactory<Louvain> {

    private Louvain.Config config;

    public LouvainFactory(Louvain.Config config) {
        this.config = config;
    }

    @Override
    public Louvain build(
            final Graph graph,
            final ProcedureConfiguration configuration,
            final AllocationTracker tracker,
            final Log log) {

        NodeProperties communityMap = config.maybeSeedPropertyKey
            .map(graph::nodeProperties)
            .orElse(null);

        return new Louvain(graph,
                config,
                communityMap,
                Pools.DEFAULT,
                configuration.getConcurrency(),
                tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        int maxLevel = config.maxLevel;
        return MemoryEstimations.builder(Louvain.class)
                .field("Config", Louvain.Config.class)
                .perNode("communities", HugeLongArray::memoryEstimation)
                .perNode("nodeWeights", HugeDoubleArray::memoryEstimation)
                .rangePerNode("dendrogram", (nodeCount) -> {
                    final long communityArraySize = HugeLongArray.memoryEstimation(nodeCount);

                    final MemoryRange innerCommunities = MemoryRange.of(
                            communityArraySize,
                            communityArraySize * maxLevel);

                    final MemoryRange communityArrayLength = MemoryRange.of(sizeOfObjectArray(1), sizeOfObjectArray(maxLevel));

                    return innerCommunities.add(communityArrayLength);
                })
                .fixed("modularities", MemoryRange.of(sizeOfDoubleArray(1), sizeOfDoubleArray(maxLevel)))
                .add("modularityOptimization", ModularityOptimization.memoryEstimation())
                .build();
    }
}
