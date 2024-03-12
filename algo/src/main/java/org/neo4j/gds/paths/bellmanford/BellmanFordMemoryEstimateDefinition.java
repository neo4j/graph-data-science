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
package org.neo4j.gds.paths.bellmanford;

import org.neo4j.gds.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;

public class BellmanFordMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final boolean trackNegativeCycles;

    public BellmanFordMemoryEstimateDefinition(boolean trackNegativeCycles) {
        this.trackNegativeCycles = trackNegativeCycles;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var builder = MemoryEstimations.builder(BellmanFord.class)
            .perNode("frontier", HugeLongArray::memoryEstimation)
            .perNode("validBitset", HugeAtomicBitSet::memoryEstimation)
            .add(DistanceTracker.memoryEstimation())
            .perThread("BellmanFordTask", BellmanFordTask.memoryEstimation());

        if (trackNegativeCycles) {
            builder.perNode("negativeCyclesVertices", HugeLongArray::memoryEstimation);
        }

        return builder.build();
    }

}
