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
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
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
    private final boolean useGap;

    public MaxFlowMemoryEstimateDefinition(long numberOfSinks, long numberOfTerminals, boolean useGap) {
        this.numberOfSinks = numberOfSinks;
        this.numberOfTerminals = numberOfTerminals;
        this.useGap = useGap;
    }

    private MemoryEstimation atomicWorkingSet() {
        return MemoryEstimations.builder(AtomicWorkingSet.class)
            .perNode("working set", HugeLongArray::memoryEstimation)
            .build();

    }

    private MemoryEstimation globalRelabellingTask() {
        return MemoryEstimations.builder(GlobalRelabellingBFSTask.class).build();
    }

    public MemoryEstimation globalRelabelling() {
        return MemoryEstimations.builder(GlobalRelabeling.class)
            .perThread("Global Relabelling task", globalRelabellingTask())
            .perGraphDimension(
                "thread queues",
                (dimensions, concurrency) -> MemoryRange.of(dimensions.nodeCount() * concurrency.value())
            )
            .add("frontier", atomicWorkingSet())
            .perNode("isDiscovered", HugeAtomicBitSet::memoryEstimation)
            .build();
    }

    public MemoryEstimation flowGraph() {
        BiFunction<GraphDimensions, Function<Long, Long>, MemoryRange> relConsumer =
            ((graphDimensions, longMemoryRangeFunction) -> {
                var newRel = graphDimensions.relCountUpperBound() + numberOfSinks + numberOfTerminals;
                return MemoryRange.of(longMemoryRangeFunction.apply(newRel));
            });
        BiFunction<GraphDimensions, Function<Long, Long>, MemoryRange> nodeConsumer =
            ((graphDimensions, longMemoryRangeFunction) -> {
                var newNodes = graphDimensions.nodeCount() + 2;
                return MemoryRange.of(longMemoryRangeFunction.apply(newNodes));
            });

        //skip revDegree array during construction because it is used only during construction
        return MemoryEstimations.builder(FlowGraph.class)
            .perNode("index offset", HugeLongArray::memoryEstimation)
            .perGraphDimension(
                "flow",
                ((dimensions, ___) -> relConsumer.apply(dimensions, HugeDoubleArray::memoryEstimation))
            )
            .perGraphDimension(
                "capacity",
                ((dimensions, ___) -> relConsumer.apply(dimensions, HugeDoubleArray::memoryEstimation))
            )
            .perGraphDimension(
                "reverse adjacency",
                ((dimensions, ___) -> relConsumer.apply(dimensions, HugeLongArray::memoryEstimation))
            )
            .perGraphDimension(
                "reverse index",
                ((dimensions, ___) -> relConsumer.apply(dimensions, HugeLongArray::memoryEstimation))
            )
            .perGraphDimension(
                "reverse offset",
                ((dimensions, ___) -> nodeConsumer.apply(dimensions, HugeLongArray::memoryEstimation))
            )

            .build();
    }

    public MemoryEstimation flowResult() {
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

    private MemoryEstimation gap() {
        return MemoryEstimations.builder(ParallelGapDetector.class)
            .rangePerGraphDimension(
                "gapRelabeling",
                (dimensions, ___) -> MemoryRange.of(HugeLongArray.memoryEstimation(dimensions.nodeCount() + 2))
            )
            .build();
    }

    private MemoryEstimation discharging() {
        var memoryBuilder = MemoryEstimations.builder(SequentialDischarging.class)
            .perNode("queue", HugeLongArrayQueue::memoryEstimation)
            .perNode("in queue", Estimate::sizeOfBitset)
            .rangePerGraphDimension(
                "filteredEdges", (dimensions, ___) -> {
                    //this is very loose: we consider a supernode where everyone is connected to a single node in both directions
                    var arcSize = Estimate.sizeOfInstance(SequentialDischarging.Arc.class);
                    var approximateMemory = HugeObjectArray.memoryEstimation(dimensions.nodeCount() * 2, arcSize);
                    return MemoryRange.of(approximateMemory);
                }
            );

        if (useGap) {
            memoryBuilder.add("gap heuristic", gap());
        }

        return memoryBuilder.build();

    }

    public MemoryEstimation maxFlowPhase() {

        BiFunction<GraphDimensions, Function<Long, Long>, MemoryRange> nodeConsumer =
            ((graphDimensions, longMemoryRangeFunction) -> {
                var newNodes = graphDimensions.nodeCount() + 2;
                return MemoryRange.of(longMemoryRangeFunction.apply(newNodes));
            });

        var memoryBuilder = MemoryEstimations.builder(MaxFlowPhase.class)
            .perNode("queue", HugeLongArrayQueue::memoryEstimation)
            .perNode("in queue", Estimate::sizeOfBitset)
            .rangePerGraphDimension(
                "label", ((dimensions, ___) -> nodeConsumer.apply(dimensions, HugeLongArray::memoryEstimation))
            )
         .rangePerGraphDimension(
            "excess", ((dimensions, ___) -> nodeConsumer.apply(dimensions, HugeDoubleArray::memoryEstimation))
        );

        if (useGap) {
            memoryBuilder.add("gap heuristic", gap());
        }

        return memoryBuilder.build();

    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(MaxFlow.class)
            .fixed("supply", Estimate.sizeOfInstance(NodeWithValue.class) * numberOfSinks)
            .fixed("demand", Estimate.sizeOfInstance(NodeWithValue.class) * numberOfTerminals)
            .add("MaxFlowPhase",maxFlowPhase())
            .add("flowGraph", flowGraph())
            .add("Discharging", discharging())
            .add("Global relabelling", globalRelabelling())
            .add("result", flowResult())
            .build();
    }
}
