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
package org.neo4j.gds.articulationpoints;

import org.neo4j.gds.bridges.Bridges;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

public class ArticulationPointsMemoryEstimateDefinition implements MemoryEstimateDefinition {
    @Override
    public MemoryEstimation memoryEstimation() {

        var builder = MemoryEstimations.builder(Bridges.class);
        builder
            .perNode("tin", HugeLongArray::memoryEstimation)
            .perNode("low", HugeLongArray::memoryEstimation)
            .perNode("children", HugeLongArray::memoryEstimation)
            .perNode("visited", Estimate::sizeOfBitset)
            .perNode("articulationPoints", Estimate::sizeOfBitset);

        builder.rangePerGraphDimension("stack", ((graphDimensions, concurrency) -> {
            long relationshipCount = graphDimensions.relCountUpperBound();
            return MemoryRange.of(
                HugeObjectArray.memoryEstimation(relationshipCount, Estimate.sizeOfInstance(ArticulationPoints.StackEvent.class))
            );


        }));

        return builder.build();
    }
}
