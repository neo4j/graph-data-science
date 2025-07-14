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
package org.neo4j.gds.cliqueCounting;

//import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
//import org.neo4j.gds.k1coloring.ColoringStep;
//import org.neo4j.gds.k1coloring.K1Coloring;
//import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
//import org.neo4j.gds.mem.MemoryEstimations;

public final class CliqueCountingMemoryEstimateDefinition implements MemoryEstimateDefinition {
    @Override
    public MemoryEstimation memoryEstimation() {
        throw new MemoryEstimationNotImplementedException();
//        return MemoryEstimations.builder(K1Coloring.class)
//            .perNode("colors", HugeLongArray::memoryEstimation)
//            .perNode("nodesToColor", Estimate::sizeOfBitset)
//            .perThread("coloring", MemoryEstimations.builder()
//                .field("coloringStep", ColoringStep.class)
//                .perNode("forbiddenColors", Estimate::sizeOfBitset)
//                .build())
//            .build();
    }
}
