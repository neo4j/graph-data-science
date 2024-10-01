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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;

public class ClusterStructure {

    private final HugeLongArray parent; //TODO: enforce path-compression heuristic
    private final HugeDoubleArray clusterPrizes; //enforce path-compression heuristic
    private final HugeLongArray left;
    private final HugeLongArray right;
    private final HugeDoubleArray moat;
    private final HugeDoubleArray totalMoat;
    private final long originalNodeCount;

    private  long maxNumberOfClusters;

    ClusterStructure(long nodeCount){

        this.parent = HugeLongArray.newArray(2*nodeCount);
        this.clusterPrizes = HugeDoubleArray.newArray(2*nodeCount);
        this.parent.fill(-1L);
        this.maxNumberOfClusters = nodeCount;
        this.totalMoat = HugeDoubleArray.newArray(2*nodeCount);
        this.moat = HugeDoubleArray.newArray(2*nodeCount);
        this.left = HugeLongArray.newArray(nodeCount);
        this.right = HugeLongArray.newArray(nodeCount);
        this.originalNodeCount = nodeCount;
    }

    long merge(long cluster1,long cluster2){
        var newCluster = maxNumberOfClusters++;
        parent.set(cluster1,newCluster);
        parent.set(cluster2,newCluster);
        clusterPrizes.set(newCluster, clusterPrizes.get(cluster1)+ clusterPrizes.get(cluster2));
        totalMoat.set(newCluster, totalMoat.get(cluster1)+ totalMoat.get(cluster2));
        left.set(newCluster- originalNodeCount, cluster1);
        right.set(newCluster- originalNodeCount, cluster2);
        return  newCluster;
    }

    void setClusterPrize(long clusterId, double prize){
        clusterPrizes.set(clusterId, prize);
    }

    double clusterPrize(long clusterId){
        return clusterPrizes.get(clusterId);
    }

    void increaseMoat(long clusterId, double val){
        moat.addTo(clusterId,val);
        totalMoat.addTo(clusterId,val);
    }

    void setMoat(long clusterId, double val){
        double  moatToAdd =  val - moat.get(clusterId);
        increaseMoat(clusterId,moatToAdd);
    }

    double  tightnessTime(long clusterId, double currentMoat){
            double  slack =   clusterPrizes.get(clusterId) - totalMoat.get(clusterId);
            return  currentMoat + slack;
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

    double sumOnEdgePartOnly(long node){
            double sum=moat.get(node);
            long currentNode =node;
            while (parent.get(currentNode)!=-1){
                currentNode = parent.get(currentNode);
                sum+= moat.get(currentNode);
            }
            return  sum;
    }

    BitSet activeOriginalNodesOfCluster(long clusterId){
        BitSet bitSet=new BitSet(originalNodeCount);

        if (clusterId < originalNodeCount){
            bitSet.set(clusterId);
            return  bitSet;
        }

        HugeLongArrayStack stack= HugeLongArrayStack.newStack(originalNodeCount);
        stack.push(clusterId);
        while (!stack.isEmpty()){
            var  stackCluster = stack.pop();
            if (stackCluster < originalNodeCount){
                bitSet.set(stackCluster);
            }else{
                var adaptedIndex = stackCluster - originalNodeCount;
                stack.push(left.get(adaptedIndex));
                stack.push(right.get(adaptedIndex));
            }
        }
        return  bitSet;
    }


}
 record ClusterMoatPair(long cluster, double totalMoat){}



