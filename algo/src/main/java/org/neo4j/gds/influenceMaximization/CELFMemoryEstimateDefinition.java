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
package org.neo4j.gds.influenceMaximization;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.MemoryUsage;

public final class CELFMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition {

    public static final int DEFAULT_BATCH_SIZE = 10;

    private final CELFParameters celfParameters;

    public CELFMemoryEstimateDefinition(CELFParameters celfParameters) {
        this.celfParameters = celfParameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(CELF.class);

        //CELF class
        builder.fixed(
                "seedSet",
                MemoryUsage.sizeOfLongDoubleScatterMap(celfParameters.seedSetSize())
            )
            .fixed("firstK", MemoryUsage.sizeOfLongArray(DEFAULT_BATCH_SIZE))
            .add("LazyForwarding: spread priority queue", HugeLongPriorityQueue.memoryEstimation())
            .perNode("greedy part: single spread array: ", HugeDoubleArray::memoryEstimation);

        //ICInitTask class

        builder
            .perThread("ICInit", ICInitMemoryEstimationBuilder().build());


        //ICLazyMC
        builder.fixed("spread", MemoryUsage.sizeOfDoubleArray(DEFAULT_BATCH_SIZE));
        //ICLazyMCTask class
        builder.add("newActive", ICLazyMemoryEstimationBuilder(celfParameters.seedSetSize()).build());
        builder.add(MemoryEstimations.builder(CELFParameters.class).build());
        
        return builder.build();
    }

    private MemoryEstimations.Builder ICInitMemoryEstimationBuilder() {
        return MemoryEstimations.builder(ICLazyForwardTask.class)
            .perNode("active", MemoryUsage::sizeOfBitset)
            .add("newActive", HugeLongArrayStack.memoryEstimation());
    }

    private MemoryEstimations.Builder ICLazyMemoryEstimationBuilder(int seedSetSize) {
        return MemoryEstimations.builder(ICLazyForwardTask.class)
            .perNode("seedActive", MemoryUsage::sizeOfBitset)
            .perNode("candidateActive", MemoryUsage::sizeOfBitset)
            .fixed("localSpread", MemoryRange.of(MemoryUsage.sizeOfDoubleArray(DEFAULT_BATCH_SIZE)))
            .fixed("candidateNodeIds", MemoryRange.of(MemoryUsage.sizeOfLongArray(DEFAULT_BATCH_SIZE)))
            .fixed("seedSetNodeIds", MemoryRange.of(MemoryUsage.sizeOfLongArray(seedSetSize)))
            .add("newActive", HugeLongArrayStack.memoryEstimation());
    }
}
