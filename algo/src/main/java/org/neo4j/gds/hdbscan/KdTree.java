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

import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.stream.LongStream;

public class KdTree {

    private final HugeLongArray ids;
    private final Distances distances;
    private final KdNode root;
    private final long treeNodeCount;

    KdTree(HugeLongArray ids, Distances distances, KdNode root, long treeNodeCount) {
        this.ids = ids;
        this.distances = distances;
        this.root = root;
        this.treeNodeCount = treeNodeCount;
    }

    KdNode root() {
        return root;
    }

    KdNode leftChild(KdNode kdNode) {
        return kdNode.leftChild();
    }

    KdNode rightChild(KdNode kdNode) {
        return kdNode.rightChild();
    }

    long  nodeAt(long index){
        return  ids.get(index);
    }

    LongStream nodesContained(KdNode node) {
        var start = node.start();
        var end = node.end();
        return LongStream.range(start, end).map(ids::get);
    }


    long  treeNodeCount(){
        return treeNodeCount;
    }

    public Neighbours neighbours(long pointId, int numberOfNeighbours) {
        var queue = new JavaUtilSearchPriorityQueue(numberOfNeighbours);
        search(root, numberOfNeighbours, queue, pointId);
        return  new Neighbours(queue.closest());
    }

    private void search(KdNode kdNode, int numberOfNeighbours, ClosestSearchPriorityQueue queue, long pointId) {
        if (kdNode.isLeaf()) {
            nodesContained(kdNode).forEach(nodeId -> {
                if ( pointId == nodeId){
                        return;
                    }
                    double distance = distances.computeDistance(pointId, nodeId);
                    var neighbour = new Neighbour(nodeId, distance);
                    queue.offer(neighbour);
                }
            );
        } else {

            var left = leftChild(kdNode);
            var right = rightChild(kdNode);

            //calculate both bounds, traverse on the best first as heuristic, possible chance to prune both!
            var leftLB = distances.lowerBound(left.aabb(),pointId);
            var rightLB = distances.lowerBound(right.aabb(),pointId);

            var firstVisit = left;
            var secondVisit = right;
            var firstBound = leftLB;
            var secondBound = rightLB;

            if (leftLB > rightLB){
                firstVisit = right;
                secondVisit = left;
                firstBound = rightLB;
                secondBound = leftLB;
            }

            if (shouldExamine(queue,numberOfNeighbours,firstBound)){
                search(firstVisit, numberOfNeighbours, queue,pointId);
                if (shouldExamine(queue,numberOfNeighbours,secondBound)) {
                    search(secondVisit, numberOfNeighbours, queue,pointId);
                }

            }

        }
    }

    private  boolean shouldExamine(ClosestSearchPriorityQueue queue, int numberOfNeighbours, double lowerBound){
        boolean shouldExamine = false;
        if(queue.size() < numberOfNeighbours) {
            shouldExamine = true;
        } else {

            shouldExamine = queue.largerThanLowerBound(lowerBound);
        }


        return  shouldExamine;
    }

}
