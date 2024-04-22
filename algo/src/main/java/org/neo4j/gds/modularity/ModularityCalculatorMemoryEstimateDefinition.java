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
package org.neo4j.gds.modularity;

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.mem.Estimate;

public class ModularityCalculatorMemoryEstimateDefinition implements MemoryEstimateDefinition {

    @Override
    public MemoryEstimation memoryEstimation() {

        //only methods inside, but want the class overhead
        var perTask = MemoryEstimations.builder(RelationshipCountCollector.class).build();

        return MemoryEstimations.builder(ModularityCalculator.class)
            .add("Community Mapper", HugeLongLongMap.memoryEstimation())
            .perNode("Inside Relationships", HugeAtomicDoubleArray::memoryEstimation)
            .perNode("Total Community Relationships", HugeAtomicDoubleArray::memoryEstimation)
            .perNode(
                "Community Modularity",
                nodeCount -> HugeObjectArray.memoryEstimation(
                    nodeCount,
                    Estimate.sizeOfInstance(CommunityModularity.class)
                )
            )
            .perThread("RelationshipCountCollector", perTask)
            .build();
    }

}
