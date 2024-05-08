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

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.Estimate;

public class DfsMemoryEstimateDefinition implements MemoryEstimateDefinition {

    @Override
    public MemoryEstimation memoryEstimation() {

        MemoryEstimations.Builder builder = MemoryEstimations.builder(DFS.class);
        builder.perNode("visited ", Estimate::sizeOfBitset);
        builder.perNode("nodes", Estimate::sizeOfLongArrayList);
        builder.perNode("sources", Estimate::sizeOfLongArrayList);
        builder.perNode("weights", Estimate::sizeOfDoubleArrayList);
        builder.perNode("resultNodes", Estimate::sizeOfLongArray);

        return builder.build();
    }

}
