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
package org.neo4j.gds.msbfs;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;

public final class MSBFSMemoryEstimation {

    private MSBFSMemoryEstimation() {}


    private  static MemoryEstimation MSBFSRunnable(boolean seenNext){
        var builder =  MemoryEstimations.builder(MultiSourceBFSRunnable.class)
            .perNode("visits", HugeLongArray::memoryEstimation)
            .perNode("visitsNext", HugeLongArray::memoryEstimation)
            .perNode("seens", HugeLongArray::memoryEstimation);

        if (seenNext) {
            builder.perNode("seenNext", HugeLongArray::memoryEstimation);
        }

        return  builder.build();

    }
    private static MemoryEstimations.Builder MSBFS(int sourceNodes, boolean seenNext){

        var builder = MemoryEstimations.builder(MultiSourceBFSAccessMethods.class);

        if (sourceNodes > 0 ) {
            builder.fixed("source nodes", Estimate.sizeOfLongArray(sourceNodes));
        }

        builder.perThread("runnable", MSBFSRunnable(seenNext));

        return builder;
    }

    public static MemoryEstimation MSBFSWithPredecessorStrategy(int sourceNodes){
        return MSBFS(sourceNodes,true).build();
    }
    public static MemoryEstimation MSBFSWithANPStrategy(int sourceNodes){
        return MSBFS(sourceNodes,false).build();
    }
}
