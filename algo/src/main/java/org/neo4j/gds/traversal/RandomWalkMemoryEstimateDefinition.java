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

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.Estimate;

public class RandomWalkMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final WalkEstimateParameters parameters;

    public RandomWalkMemoryEstimateDefinition(WalkEstimateParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var memoryUsagePerWalk = Estimate.sizeOfLongArray(parameters.walkLength());
        var sizeOfBuffer = Estimate.sizeOfObjectArray(parameters.walkBufferSize());

        var maxMemoryUsage = sizeOfBuffer + Estimate.sizeOfArray(parameters.walkBufferSize(), memoryUsagePerWalk);

        return MemoryEstimations.builder(RandomWalk.class.getSimpleName())
            .fixed("random walk buffer", MemoryRange.of(sizeOfBuffer, maxMemoryUsage))
            .build();
    }

}
