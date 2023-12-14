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
package org.neo4j.gds.paths.delta;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;

public class DeltaSteppingMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<AllShortestPathsDeltaBaseConfig> {

    @Override
    public MemoryEstimation memoryEstimation(AllShortestPathsDeltaBaseConfig configuration) {
        var storePredecessors= true;

            var builder = MemoryEstimations.builder(DeltaStepping.class)
                .perNode("distance array", HugeAtomicDoubleArray::memoryEstimation)
                .rangePerGraphDimension("shared bin", (dimensions, concurrency) -> {
                    // This is the average case since it is likely that we visit most nodes
                    // in one of the iterations due to power-law distributions.
                    var lowerBound = HugeLongArray.memoryEstimation(dimensions.nodeCount());
                    // This is the worst-case, which we will most likely never hit since the
                    // graph needs to be complete to reach all nodes from all threads.
                    var upperBound = HugeLongArray.memoryEstimation(dimensions.relCountUpperBound());

                    return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
                })
                .rangePerGraphDimension("local bins", (dimensions, concurrency) -> {
                    // We don't know how many buckets we have per thread since it depends on the delta
                    // and the average path length within the graph. We try some bounds instead ...

                    // Assuming that each node is visited by at most one thread, it is stored in at most
                    // one thread-local bucket, hence the best case is dividing all the nodes across
                    // thread-local buckets.
                    var lowerBound = HugeLongArray.memoryEstimation(dimensions.nodeCount() / concurrency);

                    // The worst case is again the fully-connected graph where we would replicate all nodes in
                    // thread-local buckets in a single iteration.
                    var upperBound = HugeLongArray.memoryEstimation(concurrency * dimensions.nodeCount());

                    return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
                });

            if (storePredecessors) {
                builder.perNode("predecessor array", HugeAtomicLongArray::memoryEstimation);
            }
            return builder.build();
        }

}
