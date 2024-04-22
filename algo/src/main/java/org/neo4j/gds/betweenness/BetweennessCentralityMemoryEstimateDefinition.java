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
package org.neo4j.gds.betweenness;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.Estimate;

import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;

public class BetweennessCentralityMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final boolean hasRelationshipWeightProperty;

    public BetweennessCentralityMemoryEstimateDefinition(boolean hasRelationshipWeightProperty) {
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
    }


    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(BetweennessCentrality.class)
            .perNode("centrality scores", HugeAtomicDoubleArray::memoryEstimation)
            .perThread(
                "compute task",
                bcTaskMemoryEstimationBuilder(hasRelationshipWeightProperty).build()
            ).build();
    }


    @NotNull
    private static MemoryEstimations.Builder bcTaskMemoryEstimationBuilder(boolean weighted) {
        var builder = MemoryEstimations.builder(BetweennessCentrality.BCTask.class)
            .add("predecessors", MemoryEstimations.setup("", (dimensions, concurrency) -> {
                // Predecessors are represented by LongArrayList which wrap a long[]
                long averagePredecessorSize = sizeOfLongArray(dimensions.averageDegree());
                return MemoryEstimations.builder(HugeObjectArray.class)
                    .perNode("array", nodeCount -> nodeCount * averagePredecessorSize)
                    .build();
            }))
            .perNode("backwardNodes", HugeLongArray::memoryEstimation)
            .perNode("deltas", HugeDoubleArray::memoryEstimation)
            .perNode("sigmas", HugeLongArray::memoryEstimation);

        if (weighted) {
            builder.add("ForwardTraverser", MemoryEstimations.setup(
                    "traverser",
                    (dimensions, concurrency) -> MemoryEstimations.builder(ForwardTraverser.class)
                        .add("nodeQueue", HugeLongPriorityQueue.memoryEstimation())
                        .perNode("visited", Estimate::sizeOfBitset)
                        .build()
                )
            );
        } else {
            builder.add("ForwardTraverser", MemoryEstimations.setup(
                    "traverser",
                    (dimensions, concurrency) -> MemoryEstimations.builder(ForwardTraverser.class)
                        .perNode("distances", HugeIntArray::memoryEstimation)
                        .perNode("forwardNodes", HugeLongArray::memoryEstimation)
                        .build()
                )
            );
        }
        return builder;
    }
}
