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
package org.neo4j.gds.paths.dijkstra;

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.MemoryUsage;

public class DijkstraMemoryEstimateDefinition implements AlgorithmMemoryEstimateDefinition<DijkstraMemoryEstimateParameters> {

    @Override
    public MemoryEstimation memoryEstimation(DijkstraMemoryEstimateParameters parameters) {
        return memoryEstimation(parameters.trackRelationships(), parameters.manyTargets()); //could be configration.track potentially
    }

   public static  MemoryEstimation memoryEstimation(boolean trackRelationships){

       return memoryEstimation(trackRelationships, false);
    }

    public static MemoryEstimation memoryEstimation(boolean trackRelationships, boolean manyTargets) {

        var builder = MemoryEstimations.builder(Dijkstra.class)
            .add("priority queue", HugeLongPriorityQueue.memoryEstimation())
            .add("reverse path", HugeLongLongMap.memoryEstimation());
        if (trackRelationships) {
            builder.add("relationship ids", HugeLongLongMap.memoryEstimation());
        }
        if (manyTargets) {
            builder.perNode("targets bitset", MemoryUsage::sizeOfBitset);
        }
        return builder
            .perNode("visited set", MemoryUsage::sizeOfBitset)
            .build();
    }

}
