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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

import java.util.function.BiFunction;
import java.util.function.Function;

public class MaxFlowMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final long numberOfSinks;
    private final long numberOfTerminals;

    public MaxFlowMemoryEstimateDefinition(long numberOfSinks, long numberOfTerminals) {
        this.numberOfSinks = numberOfSinks;
        this.numberOfTerminals = numberOfTerminals;
    }

    private MemoryEstimation atomicWorkingSet(){
        return MemoryEstimations.builder(AtomicWorkingSet.class)
            .perNode("working set", HugeLongArray::memoryEstimation)
            .build();

    }
    private MemoryEstimation globalRelabellingTask(){
        return MemoryEstimations.builder(GlobalRelabellingBFSTask.class).build();
    }
    private MemoryEstimation globalRelabelling(){
        return MemoryEstimations.builder(GlobalRelabeling.class)
            .perThread("Global Relabelling task",globalRelabellingTask())
            .add("frontier",atomicWorkingSet())
            .perNode("isDiscovered",HugeAtomicBitSet::memoryEstimation)
            .build();
    }

    private MemoryEstimation dischargeTask(){
        return MemoryEstimations.builder(DischargeTask.class).build();
    }

    private MemoryEstimation discharging(){
        return MemoryEstimations.builder(Discharging.class)
            .perThread("Discharge task", dischargeTask())
            .perNode("temp label", HugeLongArray::memoryEstimation)
            .perNode("isDiscovered",HugeAtomicBitSet::memoryEstimation)
            .build();
    }

    private MemoryEstimation flowGraph(){
        BiFunction<GraphDimensions, Function<Long,Long>,MemoryRange> relConsumer =
            ((graphDimensions, longMemoryRangeFunction) -> {
                var newRel = graphDimensions.relCountUpperBound() + numberOfSinks + numberOfTerminals;
                return  MemoryRange.of(longMemoryRangeFunction.apply(newRel));
            });
        BiFunction<GraphDimensions, Function<Long,Long>,MemoryRange> nodeConsumer =
            ((graphDimensions, longMemoryRangeFunction) -> {
                var newRel = graphDimensions.nodeCount() + 2;
                return  MemoryRange.of(longMemoryRangeFunction.apply(newRel));
            });

        //skip revDegree array during construction because it is used only during construction
        return MemoryEstimations.builder(FlowGraph.class)
            .perNode("index offset", HugeLongArray::memoryEstimation)
            .perGraphDimension("flow",((dimensions, ___) -> relConsumer.apply(dimensions, HugeDoubleArray::memoryEstimation)))
            .perGraphDimension("capacity",((dimensions, ___) -> relConsumer.apply(dimensions, HugeDoubleArray::memoryEstimation)))
            .perGraphDimension("reverse adjacency",((dimensions, ___) -> relConsumer.apply(dimensions, HugeLongArray::memoryEstimation)))
            .perGraphDimension("reverse index",((dimensions, ___) -> relConsumer.apply(dimensions, HugeLongArray::memoryEstimation)))
            .perGraphDimension("reverse offset",((dimensions, ___) -> nodeConsumer.apply(dimensions, HugeLongArray::memoryEstimation)))

            .build();
    }

    private MemoryEstimation flowResult() {
        return MemoryEstimations.builder(FlowResult.class)
            .perGraphDimension(
                "output", ((dimensions, ___) -> {
                    var sizeOfFlowRelationship = Estimate.sizeOfInstance(FlowRelationship.class);
                    return MemoryRange.of(HugeObjectArray.memoryEstimation(
                        dimensions.relCountUpperBound(),
                        sizeOfFlowRelationship
                    ));
                })
            ).build();
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(MaxFlow.class)
            .fixed("supply", Estimate.sizeOfInstance(NodeWithValue.class) * numberOfSinks)
            .fixed("demand", Estimate.sizeOfInstance(NodeWithValue.class) * numberOfTerminals)
            .perGraphDimension("thread queues", (dimensions,concurrency)-> MemoryRange.of(dimensions.nodeCount() * concurrency.value()))
            .add("flowGraph",flowGraph())
            .add("Discharging", discharging())
            .add("Global relabelling", globalRelabelling())
            .add("result", flowResult())
            .build();
    }
}
