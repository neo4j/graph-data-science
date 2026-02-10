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
package org.neo4j.gds.leiden;

import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.function.Function;

public record LeidenResult(
    HugeLongArray communities,
    int ranLevels,
    boolean didConverge,
    Function<Long,long[]> dendrograms,
    double[] modularities,
    double modularity
){

    public long[] intermediateCommunities(long nodeId) {
        return dendrograms.apply(nodeId);
    }

    public static LeidenResult createWithDendrograms(
        HugeLongArray communities,
        int ranLevels,
        boolean didConverge,
        LeidenDendrogramManager dendrogramManager,
        double[] modularities,
        double modularity
    ){
        return new LeidenResult(
            communities,
            ranLevels,
            didConverge,
            (nodeId)->{
                var dendrograms = dendrogramManager.getAllDendrograms();
                long[] comms = new long[ranLevels];
                for (int i = 0; i < ranLevels; i++) {
                    comms[i] = dendrograms[i].get(nodeId);
                }
                return comms;
            },
            modularities,
            modularity
        );

    }

    public static LeidenResult createWithoutDendrograms(
        HugeLongArray communities,
        int ranLevels,
        boolean didConverge,
        double[] modularities,
        double modularity
    ){
        return new LeidenResult(
            communities,
            ranLevels,
            didConverge,
            (nodeId)-> new long[]{communities.get(nodeId)},
            modularities,
            modularity
        );

    }

    public static LeidenResult EMPTY =  LeidenResult.createWithoutDendrograms(
        HugeLongArray.newArray(0),
        0,
        false,
        new double[0],
        0
    );

}
