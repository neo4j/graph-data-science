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

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
//import org.neo4j.gds.mem.MemoryEstimations;

public final class CliqueCountingMemoryEstimateDefinition implements MemoryEstimateDefinition {

    public CliqueCountingMemoryEstimateDefinition() {
    }

    @Override
    public MemoryEstimation memoryEstimation() {

        return  MemoryEstimations.builder(CliqueCounting.class)
            .perThread("intersection cost per thread",  thread())
            .build();
    }

    private MemoryEstimation thread(){
            return MemoryEstimations.builder()
                .rangePerGraphDimension("Intersections",((graphDimensions, concurrency) -> {
                    var  bound1 = (long)(graphDimensions.averageDegree()*2.7);
                    var bound2  = (long) (0.48*Math.sqrt(graphDimensions.relCountUpperBound())); //a theoretical bound  ¯\_(ツ)_/¯
                    var cost1 = cost(bound1);
                    var cost2 = cost(bound2);
                    return MemoryRange.of(Math.min(cost1, cost2), Math.max(cost1, cost2));
                }))
                .build();

    }

    private long cost(long bound) {
        //very very loose bounds
        long recursionLevels = bound;
        long intersectionsPerLevel = (bound * (bound-1))/2;
        return recursionLevels * intersectionsPerLevel;
    }

}
