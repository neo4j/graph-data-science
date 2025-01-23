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
import org.neo4j.gds.core.utils.Intersections;

import java.util.OptionalLong;
import java.util.stream.LongStream;

public class KdTree {

    private final HugeLongArray ids;
    private final NodePropertyValues nodePropertyValues;
    private final KdNode root;
    private final long treeNodeCount;

    KdTree(HugeLongArray ids, NodePropertyValues nodePropertyValues, KdNode root, long treeNodeCount) {
        this.ids = ids;
        this.nodePropertyValues = nodePropertyValues;
        this.root = root;
        this.treeNodeCount = treeNodeCount;
    }

    KdNode root() {
        return root;
    }

    KdNode parent(KdNode kdNode) {
        return kdNode.parent();
    }
    
    KdNode leftChild(KdNode kdNode) {
        return kdNode.leftChild();
    }

    KdNode rightChild(KdNode kdNode) {
        return kdNode.rightChild();
    }

    KdNode sibling(KdNode kdNode) {
        return kdNode.sibling();
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

    // TODO: maybe overloads for the different array types 🤔
    Neighbour[] neighbours(double[] queryPoint, int numberOfNeighbours) {
        var queue =new JavaUtilSearchPriorityQueue(numberOfNeighbours);
        search(root, queryPoint, numberOfNeighbours, queue, OptionalLong.empty());
        return queue.closest();
    }

    Neighbour[] neighbours(long pointId, int numberOfNeighbours) {
        var queue =new JavaUtilSearchPriorityQueue(numberOfNeighbours);
        var queryPoint = nodePropertyValues.doubleArrayValue(pointId);

        search(root, queryPoint, numberOfNeighbours, queue, OptionalLong.of(pointId));
        return queue.closest();
    }

    private void search(KdNode kdNode, double[] queryPoint, int numberOfNeighbours, ClosestSearchPriorityQueue queue, OptionalLong pointId) {
        if (kdNode.isLeaf()) {
            nodesContained(kdNode).forEach(nodeId -> {
                    if ((pointId.orElse(-1L)==nodeId)){
                        return;
                    }
                    var point = nodePropertyValues.doubleArrayValue(nodeId);
                    double distance = Intersections.sumSquareDelta(point, queryPoint);
                    var neighbour = new Neighbour(nodeId, distance);
                    queue.offer(neighbour);
                }
            );
        } else {
            var splitInformation = kdNode.splitInformation();
            var d = splitInformation.dimension();

            var childOnPath = leftChild(kdNode);
            var sibling =rightChild(kdNode);
            if (queryPoint[d] >= splitInformation.median()) {
                childOnPath = rightChild(kdNode);
                sibling = leftChild(kdNode);
            }
            search(childOnPath, queryPoint, numberOfNeighbours, queue,pointId);
            boolean shouldExamineOtherSide = false;
                if(queue.size() < numberOfNeighbours) {
                    shouldExamineOtherSide = true;
                } else {
                    var distance = sibling.aabb().lowerBoundFor(queryPoint);
                     shouldExamineOtherSide = queue.largerThanLowerBound(distance);
                }
                if (shouldExamineOtherSide){
                    search(sibling, queryPoint, numberOfNeighbours, queue,pointId);
                }
        }
    }


}
