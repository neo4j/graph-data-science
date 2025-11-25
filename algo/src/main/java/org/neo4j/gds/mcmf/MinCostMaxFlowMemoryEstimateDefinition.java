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
package org.neo4j.gds.mcmf;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.maxflow.GlobalRelabeling;
import org.neo4j.gds.maxflow.MaxFlow;
import org.neo4j.gds.maxflow.MaxFlowMemoryEstimateDefinition;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

public class MinCostMaxFlowMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final long numberOfSinks;
    private final long numberOfTerminals;
    private final MaxFlowMemoryEstimateDefinition maxFlowMemoryEstimateDefinition;

    public MinCostMaxFlowMemoryEstimateDefinition(long numberOfSinks, long numberOfTerminals) {
        this.numberOfSinks = numberOfSinks;
        this.numberOfTerminals = numberOfTerminals;
        this.maxFlowMemoryEstimateDefinition = new MaxFlowMemoryEstimateDefinition(numberOfSinks, numberOfTerminals, true);
    }


    private MemoryEstimation globalRelabelling() {
        return MemoryEstimations.builder(GlobalRelabeling.class)
            .perNode("frontier", HugeLongArrayQueue::memoryEstimation)
            .perNode("isFound", Estimate::sizeOfBitset)
            .add("priorityQueue", HugeLongPriorityQueue.memoryEstimation())
            .build();
    }

    private MemoryEstimation costFlowGraph() {
        return MemoryEstimations.builder(CostFlowGraph.class)
            .add("flowGraph", maxFlowMemoryEstimateDefinition.flowGraph())
            .perGraphDimension("costs", ((graphDimensions, __) -> MemoryRange.of(HugeLongArray.memoryEstimation(graphDimensions.relCountUpperBound())))) //todo: size?? (64 bits)
            .build();
    }

    private MemoryEstimation costFlowResult() {
        return MemoryEstimations.builder(CostFlowResult.class)
            .add("costFlowResult", maxFlowMemoryEstimateDefinition.flowResult())
            .build();
    }

    private MemoryEstimation costDischarging() {
        return MemoryEstimations.builder(CostDischarging.class)
            .perNode("queue", HugeLongArrayQueue::memoryEstimation)
            .perNode("inQueue", Estimate::sizeOfBitset)
            .rangePerGraphDimension(
                "filteredEdges", (dimensions, __) -> {
                    var arcSize = Estimate.sizeOfInstance(CostDischarging.Arc.class);
                    var approximateMemory = HugeObjectArray.memoryEstimation(dimensions.nodeCount() * 2, arcSize);
                    return MemoryRange.of(approximateMemory);
                }
            ).build();
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(MaxFlow.class)
            .fixed("supply", Estimate.sizeOfInstance(NodeWithValue.class) * numberOfSinks)
            .fixed("demand", Estimate.sizeOfInstance(NodeWithValue.class) * numberOfTerminals)
            .add("maxFlowPhase", maxFlowMemoryEstimateDefinition.maxFlowPhase())
            .add("maxFlow globalRelabelling", maxFlowMemoryEstimateDefinition.globalRelabelling())
            .add("costFlowGraph", costFlowGraph())
            .perNode("prize", HugeDoubleArray::memoryEstimation)
            .add("costDischarging", costDischarging())
            .add("globalRelabelling", globalRelabelling())
            .add("result", costFlowResult())
            .build();
    }
}
