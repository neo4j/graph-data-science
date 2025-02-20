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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongToDoubleFunction;

class KdTreeNodeBuilderTask implements Runnable {

    private final HugeLongArray ids;
    private final NodePropertyValues nodePropertyValues;
    private final long start;
    private final long end;
    private final long maxLeafSize;
    private final int pointSize;
    private KdNode kdNode;
    private final boolean amLeftChild;
    private final KdNode parent;
    private final AtomicInteger  nodeIndex;
    private final ProgressTracker progressTracker;


    KdTreeNodeBuilderTask(
        HugeLongArray ids,
        NodePropertyValues nodePropertyValues,
        long start,
        long end,
        long maxLeafSize,
        boolean amLeftChild,
        KdNode parent,
        AtomicInteger nodeIndex,
        ProgressTracker progressTracker
    ) {
        this.ids = ids;
        this.nodePropertyValues = nodePropertyValues;
        this.start = start;
        this.end = end;
        this.maxLeafSize = maxLeafSize;
        this.pointSize = nodePropertyValues.dimension().orElse(-1);
        this.amLeftChild = amLeftChild;
        this.parent = parent;
        this.nodeIndex = nodeIndex;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        var nodeSize = end - start;
        var aabb  = AABB.create(nodePropertyValues, ids, start, end, pointSize);
        var treeNodeId = nodeIndex.getAndIncrement();
        if (nodeSize <= maxLeafSize) {
            kdNode = KdNode.createLeaf(treeNodeId, start, end, aabb);
            progressTracker.logProgress(nodeSize);
        } else {

            int indexToSplit = aabb.mostSpreadDimension(); //step. 1: find the index to  dimension split
            long median = findMedianAndSplit(indexToSplit);  //step.2  modify array so that everything < is before median and everything >= after
            var medianValue = nodePropertyValues.doubleArrayValue(ids.get(median-1))[indexToSplit];

            kdNode = KdNode.createSplitNode(treeNodeId, start,end,aabb,new SplitInformation(medianValue,indexToSplit));
            //TODO: step.4 add these builder tasks into a fork-join
            var leftChildBuilder = new KdTreeNodeBuilderTask(ids,
                nodePropertyValues,
                start,
                median,
                maxLeafSize,
                true,
                kdNode,
                nodeIndex,
                progressTracker
            );
            leftChildBuilder.run();

            var rightChildBuilder = new KdTreeNodeBuilderTask(
                ids,
                nodePropertyValues,
                median,
                end,
                maxLeafSize,
                false,
                kdNode,
                nodeIndex,
                progressTracker
            );

            rightChildBuilder.run();

            var leftChild = leftChildBuilder.kdNode();
            var rightChild = rightChildBuilder.kdNode();

            //Observation A: assuming left/right are handled otuside this call, the below will not work, need a way to handle siblings
            //one idea: associate  an atomic integer with sum 2,  to these tasks, each decrease by 0, the one that gets it to 0 sets the sibling link
            leftChild.sibling(rightChild);
            rightChild.sibling(leftChild);
        }
        if (parent!=null){ //but this will work (wrt observation A)
            if (amLeftChild){
                kdNode.parent(parent);
                parent.leftChild(kdNode);
            }else{
                kdNode.parent(parent);
                parent.rightChild(kdNode);
            }
        }
    }

    KdNode kdNode(){
        return  kdNode;
    }
    long findMedianAndSplit(int dimensionIndex) {
        LongToDoubleFunction valueAt = v -> nodePropertyValues.doubleArrayValue(ids.get(v))[dimensionIndex];
        long l = start;
        long r = end;
        long medianIndex = (long) Math.ceil((l + r) / 2.0);
        /*
         *  the split does not have to be 'perfect':
         * i.e., left child gets all <median and right side all >= median (1)
         *  --
         * for example assume: 1,2,2,2,2........,2,3
         * --
         * if everything is sorted: the half element is a 2, but there is not split such that  (1) is satisfied. and each size
         * has size half.
         * but our purpose is just to split, and eventually perform nearest neighbors queries, which will anyway
         *
         * most probbly consider sibling nodes so we can leave with such a situaion i.e., if both produced children have the same value
         * in each dimension
         */
        while (true) {
            var currentIndex = partition(valueAt, l, r);
            if (currentIndex == medianIndex) {
                return medianIndex;
            } else if (currentIndex < medianIndex) {
                l = currentIndex + 1;
            } else {
                r = currentIndex;
            }
        }
    }

    private long partition(LongToDoubleFunction valueAt, long l, long r) {
        var pIndex = l;
        double pivot = valueAt.applyAsDouble(r - 1);
        for (var i = l; i < r; i++) {
            double vi = valueAt.applyAsDouble(i);
            if (vi < pivot) {
                var tmp = ids.get(i);
                ids.set(i, ids.get(pIndex));
                ids.set(pIndex, tmp);
                pIndex++;
            }
        }

        var tmp = ids.get(r - 1);
        ids.set(r - 1, ids.get(pIndex));
        ids.set(pIndex, tmp);

        return pIndex;
    }


}
