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

import java.util.function.LongPredicate;

public class ClusterStructure {

    private final HugeDoubleArray skippedParentSum;
    private final HugeLongArray parent;
    private final HugeDoubleArray initialMoatLeft;
    private final HugeLongArray left;
    private final HugeLongArray right;
    private final HugeDoubleArray moat;
    private final long originalNodeCount;
    private final ClusterActivity clusterActivity;
    private  long maxNumberOfClusters;

    ClusterStructure(long nodeCount){

        this.parent = HugeLongArray.newArray(2*nodeCount);
        this.initialMoatLeft = HugeDoubleArray.newArray(2*nodeCount);
        this.parent.fill(-1L);
        this.maxNumberOfClusters = nodeCount;
        this.skippedParentSum = HugeDoubleArray.newArray(2*nodeCount);
        this.moat = HugeDoubleArray.newArray(2*nodeCount);
        this.left = HugeLongArray.newArray(nodeCount);
        this.right = HugeLongArray.newArray(nodeCount);
        this.originalNodeCount = nodeCount;
        this.clusterActivity = new ClusterActivity(nodeCount);
    }

    long merge(long cluster1,long cluster2,double moat){
        var newCluster = maxNumberOfClusters++;
        parent.set(cluster1,newCluster);
        parent.set(cluster2,newCluster);
        var moat1 = moatAt(cluster1,moat);
        var moat2 = moatAt(cluster2,moat);
        initialMoatLeft.set(newCluster, initialMoatLeft.get(cluster1)+ initialMoatLeft.get(cluster2) - moat1- moat2);

        long newClusterAdaptedIndex = newCluster - originalNodeCount;

        deactivateCluster(cluster1,moat);
        deactivateCluster(cluster2,moat);

        clusterActivity.activateCluster(newCluster,moat);
        left.set(newClusterAdaptedIndex, cluster1);
        right.set(newClusterAdaptedIndex, cluster2);
        return  newCluster;
    }
    void  deactivateCluster(long clusterId, double moat){
        if (clusterActivity.active(clusterId)) {
            this.moat.set(clusterId,moatAt(clusterId,moat));
            clusterActivity.deactivateCluster(clusterId, moat);
        }
    }

    void setClusterPrize(long clusterId, double prize){
        initialMoatLeft.set(clusterId, prize);
    }

    double moatLeft(long clusterId){
        return initialMoatLeft.get(clusterId);
    }

    double  tightnessTime(long clusterId, double currentMoat){
            double  slack =  initialMoatLeft.get(clusterId) - moatAt(clusterId,currentMoat);
            return  currentMoat + slack;
    }

    void sumOnEdgePart(long node, double currentMoat, ClusterMoatPair clusterMoatPair){
        double sum = 0;
        long currentNode = node;

        while (true){

            var  parentNode = parent.get(currentNode);
            double currentValue = moatAt(currentNode,currentMoat);

            sum+= currentValue;
            if (parentNode== -1){
                break;
            }else{
                var nextNextParent =  parent.get(parentNode);
                double parentValue = moatAt(parentNode, currentMoat);
                var nextParent =  parentNode;
                if (nextNextParent !=-1){
                    parent.set(currentNode, nextNextParent);
                    skippedParentSum.addTo(currentNode, parentValue + skippedParentSum.get(parentNode));
                    nextParent= nextNextParent;
                }
                sum += skippedParentSum.get(currentNode);
                currentNode = nextParent;
            }

        }
        clusterMoatPair.assign(currentNode,sum);
    }

    BitSet activeOriginalNodesOfCluster(long clusterId){
        BitSet bitSet = new BitSet(originalNodeCount);

        if (clusterId < originalNodeCount){
            bitSet.set(clusterId);
            return bitSet;
        }

        HugeLongArrayStack stack = HugeLongArrayStack.newStack(originalNodeCount);
        stack.push(clusterId);

        while (!stack.isEmpty()){
            var  stackCluster = stack.pop();
                var adaptedIndex = stackCluster - originalNodeCount;
                var leftChild = left.get(adaptedIndex);
                var rightChild = right.get(adaptedIndex);
                if (leftChild  < originalNodeCount) {
                    bitSet.set(leftChild);
                }else{
                    stack.push(leftChild);
                }
                if (rightChild  < originalNodeCount) {
                    bitSet.set(rightChild);
                }else{
                    stack.push(rightChild);
                }

        }
        return  bitSet;
    }

     double moatAt(long clusterId, double moat){
        if (!clusterActivity.active(clusterId)) {
            return this.moat.get(clusterId);
        }
        return  moat -  clusterActivity.relevantTime(clusterId);
    }

    long numberOfActiveClusters(){
        return  clusterActivity.numberOfActiveClusters();
    }

    LongPredicate active(){
        return clusterActivity.active();
    }

    boolean active(long clusterId){
        return clusterActivity.active(clusterId);
    }

    long singleActiveCluster(){
        return  clusterActivity.firstActiveCluster();
    }

    double inactiveSince(long clusterId){
        return  clusterActivity.relevantTime(clusterId);
    }



}



