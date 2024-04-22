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
package org.neo4j.gds.steiner;

import org.neo4j.gds.MemoryEstimateDefinition;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.mem.Estimate;

public class SteinerTreeMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final boolean applyRerouting;

    public SteinerTreeMemoryEstimateDefinition(boolean applyRerouting) {
        this.applyRerouting = applyRerouting;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        var memoryEstimationBuilder = MemoryEstimations.builder()
            .perNode("terminal bitset", Estimate::sizeOfBitset)
            .perNode("parent", HugeLongArray::memoryEstimation)
            .perNode("parent cost ", HugeDoubleArray::memoryEstimation)

            .add(SteinerBasedDeltaStepping.memoryEstimation());

        if (applyRerouting) {
            memoryEstimationBuilder.perNode("queue", HugeLongArrayQueue::memoryEstimation);
            memoryEstimationBuilder.add(SimpleRerouter.estimation());
        }

        return memoryEstimationBuilder.build();
    }

}
