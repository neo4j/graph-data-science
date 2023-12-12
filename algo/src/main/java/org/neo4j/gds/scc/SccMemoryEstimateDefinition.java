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
package org.neo4j.gds.scc;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.PagedLongStack;
import org.neo4j.gds.mem.MemoryUsage;

public class SccMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<SccCommonBaseConfig> {

    @Override
    public MemoryEstimation memoryEstimation(SccCommonBaseConfig configuration) {
        var builder = MemoryEstimations.builder(Scc.class);
        builder
            .perNode("index", HugeLongArray::memoryEstimation)
            .perNode("connectedComponents", HugeLongArray::memoryEstimation)
            .perNode("visited", MemoryUsage::sizeOfBitset)
            .add("boundaries", HugeLongArrayStack.memoryEstimation())
            .add("stack", HugeLongArrayStack.memoryEstimation());

        builder.rangePerGraphDimension("todo", ((graphDimensions, concurrency) -> {
            long nodeCount = graphDimensions.nodeCount();
            long relationshipCount = graphDimensions.relCountUpperBound();
            return MemoryRange.of(
                PagedLongStack.memoryEstimation(nodeCount),
                PagedLongStack.memoryEstimation(Math.max(nodeCount, relationshipCount))
                //this bound is very-very-very loose
            );
        }));

        return builder.build();
    }

}
