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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public class BoruvkaMST extends Algorithm<GeometricMSTResult> {

    private final NodePropertyValues nodePropertyValues;
    private final KdTree kdTree;
    private final BitSet kdNodeSingleComponent;
    private final ClosestDistanceInformationTracker closestDistanceTracker;
    private final HugeDoubleArray coreValues;

    private final DisjointSetStruct unionFind;
    private final long nodeCount;
    private final Concurrency concurrency;

    private final HugeObjectArray<Edge> edges;
    private long edgeCount = 0;
    private double totalEdgeSum = 0d;


    private BoruvkaMST(
        NodePropertyValues nodePropertyValues,
        KdTree kdTree,
        ClosestDistanceInformationTracker closestDistanceTracker,
        HugeDoubleArray coreValues,
        long nodeCount, Concurrency concurrency
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.nodePropertyValues = nodePropertyValues;
        this.closestDistanceTracker = closestDistanceTracker;
        this.kdTree = kdTree;
        this.kdNodeSingleComponent = new BitSet(kdTree.treeNodeCount());

        this.coreValues = coreValues;
        //for now use existing tool
        this.unionFind = new HugeAtomicDisjointSetStruct(nodeCount, new Concurrency(1));

        this.edges = HugeObjectArray.newArray(Edge.class, nodeCount - 1);
        this.nodeCount = nodeCount;
        this.concurrency = concurrency;
    }


    public static BoruvkaMST createWithZeroCores(
        NodePropertyValues nodePropertyValues,
        KdTree kdTree,
        long nodeCount,
        Concurrency concurrency
    ) {
        var zeroCores = HugeDoubleArray.newArray(nodeCount);

        return new BoruvkaMST(
            nodePropertyValues,
            kdTree,
            ClosestDistanceInformationTracker.create(nodeCount),
            zeroCores,
            nodeCount,
            concurrency
        );
    }

    public static BoruvkaMST create(
        NodePropertyValues nodePropertyValues,
        KdTree kdTree,
        CoreResult coreResult,
        long nodeCount,
        Concurrency concurrency
    ) {
        var cores = coreResult.createCoreArray();
        var closestTracker = ClosestDistanceInformationTracker.create(nodeCount, cores, coreResult);

        return new BoruvkaMST(nodePropertyValues, kdTree, closestTracker, cores, nodeCount,concurrency);
    }


    @Override
    public GeometricMSTResult compute() {
        var kdRoot = kdTree.root();
        var rootId = kdRoot.id();
        while (!kdNodeSingleComponent.get(rootId)) {
            performIteration();
        }
        return new GeometricMSTResult(edges, totalEdgeSum);
    }

    private void performIteration() {
        if (!closestDistanceTracker.isUpdated()) {

            ParallelUtil.parallelForEachNode(
                nodeCount, concurrency, terminationFlag,
                (q) -> {
                    var   qArray = nodePropertyValues.doubleArrayValue(q);
                    var   qComp = unionFind.setIdOf(q);
                    traversalStep(q,kdTree.root(),qComp,0,qArray);
                }
            );
        }

        mergeComponents();
        //reset bounds
        // TODO: find the component id to reset up to?
        closestDistanceTracker.reset(unionFind.size());
        updateSingleComponent(kdTree.root());
    }

    private boolean filterNodesOnCoreValue(long node, long component) {
        return coreValues.get(node) < closestDistanceTracker.componentClosestDistance(component);
    }

    boolean prune(KdNode kdNode, long componentId, double  lowerBoundOnDistance){
        var  nodeComponent = singleComponentOr(kdNode,-1);
        if (nodeComponent == componentId) return true;
        var  currentComponentBest = closestDistanceTracker.componentClosestDistance(componentId);
        return  currentComponentBest < lowerBoundOnDistance;
    }

    boolean tryUpdate(long qComp, long q,long r, double distance){
        return closestDistanceTracker.tryToAssign(qComp,q,r,distance);
    }

    double baseCase(long q,long r, double[] qArray, long qComp){
        var rComp = unionFind.setIdOf(r);
        if (rComp != qComp && filterNodesOnCoreValue(r, qComp)) {
            var rArray =  nodePropertyValues.doubleArrayValue(r);
            var rqDistance = Intersections.sumSquareDelta(qArray, rArray);
            var adaptedDistance = Math.max(Math.max(coreValues.get(r), coreValues.get(q)), rqDistance);
            if (tryUpdate(qComp,q,r,adaptedDistance)){
              return adaptedDistance;
          }
        }
        return  -1;
    }

     void traversalStep(long q, KdNode kdNode, long qComp, double  lowerBoundOnDistance,double[] qArray){
        if (!prune(kdNode,qComp,lowerBoundOnDistance)) {
            if (kdNode.isLeaf()) {
                var start = kdNode.start();
                var end = kdNode.end();
                for (long index = start; index < end; ++index) {
                    baseCase(q,kdTree.nodeAt(index),qArray,qComp);
                }
            }else{
                var left = kdTree.leftChild(kdNode);
                var right = kdTree.rightChild(kdNode);
                var lowerBoundLeft = left.aabb().lowerBoundFor(qArray);
                lowerBoundLeft*=lowerBoundLeft;
                var lowerBoundRight = right.aabb().lowerBoundFor(qArray);
                lowerBoundRight*=lowerBoundRight;

                if (lowerBoundRight < lowerBoundLeft){
                    traversalStep(q,right,qComp,lowerBoundRight,qArray);
                    traversalStep(q,left,qComp,lowerBoundLeft,qArray);
                }else{
                    traversalStep(q,left,qComp,lowerBoundLeft,qArray);
                    traversalStep(q,right,qComp,lowerBoundRight,qArray);
                }
            }
        }

    }

    long singleComponentOr(KdNode node, long or) {
        long id = node.id();
        if (!kdNodeSingleComponent.get(id)) return or; //this is a trick to return distinct id
        return unionFind.setIdOf(kdTree.nodeAt(node.start()));

    }

    void mergeComponents() {
        for (var componentId = 0; componentId < nodeCount; componentId++) {
            var u = closestDistanceTracker.componentInsideBestNode(componentId);
            var v = closestDistanceTracker.componentOutsideBestNode(componentId);
            if (u == -1 || v == -1) {
                continue;
            }

            var uComponent = unionFind.setIdOf(u);
            var vComponent = unionFind.setIdOf(v);

            if (uComponent == vComponent) {
                closestDistanceTracker.resetComponent(componentId);
                continue;
            }

            var distance = Math.sqrt(closestDistanceTracker.componentClosestDistance(componentId));
            this.edges.set(
                edgeCount,
                new Edge(u, v, distance)
            );
            this.edgeCount++;
            this.totalEdgeSum += distance;

            unionFind.union(uComponent, vComponent);
        }

    }

    boolean updateSingleComponent(KdNode node) {
        long id = node.id();
        if (kdNodeSingleComponent.get(id)) {
            return true;
        }
        if (node.isLeaf()) {
            var start = node.start();
            var end = node.end();
            long expected = unionFind.setIdOf(kdTree.nodeAt(start));

            boolean isSingle = true;
            for (var ptr = start + 1; ptr < end; ++ptr) {
                if (unionFind.setIdOf(kdTree.nodeAt(ptr)) != expected) {
                    isSingle = false;
                    break;
                }
            }
            if (isSingle) {
                kdNodeSingleComponent.set(id);
            }
            return isSingle;
        } else {
            var left = kdTree.leftChild(node);
            var right = kdTree.rightChild(node);
            if (updateSingleComponent(left) && updateSingleComponent(right)) {
                var singleLeft = singleComponentOr(left, -1);
                var singleRight = singleComponentOr(right, -2);
                boolean isSingle = singleRight == singleLeft;
                if (isSingle) {
                    kdNodeSingleComponent.set(id);
                }
                return isSingle;
            }
            return false;

        }
    }

    void mergeComponents(long comp0, long comp1) {
        unionFind.union(comp0, comp1);
    }

}
