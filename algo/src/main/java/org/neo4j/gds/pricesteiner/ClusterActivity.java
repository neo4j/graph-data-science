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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.ha.HugeDoubleArray;

import java.util.function.LongPredicate;

 class ClusterActivity {

    private final BitSet activeClusters;
    //TODO: we should not need to store this as we have the moat from the `ClusterStructure`, doing this now for the first draft.
    private  final HugeDoubleArray inactiveSince;
    private  long numberOfActiveClusters;

     ClusterActivity(long nodeCount){
        this.activeClusters = new BitSet(2*nodeCount);
        this.inactiveSince   = HugeDoubleArray.newArray(2*nodeCount);
         inactiveSince.fill(-1);
         this.numberOfActiveClusters =nodeCount;
    }

    void deactivateCluster(long clusterId, double moat){
        activeClusters.clear(clusterId);
        inactiveSince.set(clusterId, moat);
        this.numberOfActiveClusters--;
    }

    void activateCluster(long clusterId){
         activeClusters.set(clusterId);
         this.numberOfActiveClusters++;
    }

    long numberOfActiveClusters(){
         return numberOfActiveClusters;
    }
    double inactiveSince(long clusterId){
         return inactiveSince.get(clusterId);
    }
    LongPredicate active(){
         return  activeClusters::get;
    }

    boolean active(long clusterId){
         return activeClusters.get(clusterId);
    }

    long firstActiveCluster(){
         return  activeClusters.nextSetBit(0);
    }


}
