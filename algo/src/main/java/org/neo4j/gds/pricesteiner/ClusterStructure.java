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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

public class ClusterStructure {

    private final HugeLongArray parent;
    private final HugeDoubleArray moat;
    private  long maxNumberOfClusters;
    ClusterStructure(long nodeCount){

        this.parent = HugeLongArray.newArray(2*nodeCount);
        this.parent.fill(-1L);
        this.maxNumberOfClusters = nodeCount;

        this.moat = HugeDoubleArray.newArray(2*nodeCount);
    }

    long merge(long componentA,long componentB){
        var newCluster = maxNumberOfClusters++;
        parent.set(componentA,newCluster);
        parent.set(componentB,newCluster);
        return  newCluster;
    }

    void increaseMoat(long componentId, double val){
        moat.addTo(componentId,val);
    }



    ClusterMoatPair sumOnEdgePart(long node){
        double sum=moat.get(node);
        long currentNode =node;
        while (parent.get(currentNode)!=-1){
            currentNode = parent.get(currentNode);
            sum+= moat.get(currentNode);
        }
        return new ClusterMoatPair(currentNode,sum);
    }
}
 record ClusterMoatPair(long cluster, double totalMoat){}
