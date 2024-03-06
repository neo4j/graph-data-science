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
package org.neo4j.gds.paths.traverse;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.mem.MemoryUsage;

public class BfsMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition {

    @Override
    public MemoryEstimation memoryEstimation() {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(BFS.class);

        builder.perNode("visited ", HugeAtomicBitSet::memoryEstimation) //global variables
            .perNode("traversedNodes", HugeLongArray::memoryEstimation)
            .perNode("weights", HugeDoubleArray::memoryEstimation)
            .perNode("minimumChunk", HugeAtomicLongArray::memoryEstimation);

        //per thread
        builder.rangePerGraphDimension("localNodes", (dimensions, concurrency) -> {
            // lower-bound: each node is in exactly one localNode array
            var lowerBound = MemoryUsage.sizeOfLongArrayList(dimensions.nodeCount() + dimensions.nodeCount() / 64);

            //In the upper bound, we can consider two scenarios:
            //  -each node except the starting will be added by every thread exactly once
            //  -traversing each relationship creates an entry in any localNodes array of one of the threads
            //We can take the minimum of these as a more accurate upper bound.
            //Nonetheless, either of those scenarios is unlikely to happen because all nodes in all threads
            //need to be added at the exact same step to force such memory usage in an extremely convoluted way
            var maximumTotalSizeOfAggregatedLocalNodes = Math.min(
                dimensions.relCountUpperBound(),
                concurrency * (dimensions.nodeCount() - 1)
            );

            var upperBound = MemoryUsage.sizeOfLongArrayList(maximumTotalSizeOfAggregatedLocalNodes + dimensions.nodeCount() / 64);
            //The  nodeCount()/64 refers to the  chunk separator in localNodes
            return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
        }).perGraphDimension("chunks", (dimensions, concurrency) ->
            MemoryRange.of(dimensions.nodeCount() / 64)
        );

        builder.perNode("resultNodes", HugeLongArray::memoryEstimation);


        return builder.build();
    }

}
