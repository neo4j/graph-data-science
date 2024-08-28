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
package org.neo4j.gds.traversal;

import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;

public class RandomWalkCountingVisitsMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final WalkEstimateParameters parameters;

    public RandomWalkCountingVisitsMemoryEstimateDefinition(WalkEstimateParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var memoryUsagePerWalk = Estimate.sizeOfLongArray(parameters.walkLength());

        return MemoryEstimations.builder(RandomWalkCountingNodeVisits.class.getSimpleName())
            .perNode("visits",HugeAtomicLongArray::memoryEstimation)
            .perThread("local walk", memoryUsagePerWalk)
            .build();

    }

}
