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

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.Estimate;

public class DijkstraMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final DijkstraMemoryEstimateParameters parameters;

    public DijkstraMemoryEstimateDefinition(DijkstraMemoryEstimateParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        boolean trackRelationships = parameters.trackRelationships();
        boolean manyTargets = parameters.manyTargets();

        var builder = MemoryEstimations.builder(Dijkstra.class)
            .add("priority queue", HugeLongPriorityQueue.memoryEstimation())
            .add("reverse path", HugeLongLongMap.memoryEstimation());
        if (trackRelationships) {
            builder.add("relationship ids", HugeLongLongMap.memoryEstimation());
        }
        if (manyTargets) {
            builder.perNode("targets bitset", Estimate::sizeOfBitset);
        }
        return builder
            .perNode("visited set", Estimate::sizeOfBitset)
            .build();
    }

}
