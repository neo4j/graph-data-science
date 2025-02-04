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
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.stream.LongStream;

public class DualTreeMSTAlgorithm extends Algorithm<HugeObjectArray<Edge>> {

    private final NodePropertyValues nodePropertyValues;
    private final KdTree kdTree;
    private final HugeDoubleArray kdNodeBound;
    private final ClosestDistanceInformationTracker closestDistanceTracker;
    private final BitSet kdNodeSingleComponent;
    private final HugeDoubleArray coreValues;
    private final DisjointSetStruct unionFind;
    private final long nodeCount;

    private final HugeObjectArray<Edge> edges;
    private long edgeCount = 0;

    DualTreeMSTAlgorithm(
        NodePropertyValues nodePropertyValues,
        KdTree kdTree,
        HugeDoubleArray coreValues,
        long nodeCount
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.nodePropertyValues = nodePropertyValues;
        this.kdNodeBound = HugeDoubleArray.newArray(kdTree.treeNodeCount());
        this.closestDistanceTracker = ClosestDistanceInformationTracker.create(nodeCount);
        this.kdTree = kdTree;
        this.kdNodeSingleComponent = new BitSet(kdTree.treeNodeCount());

        // TODO: `coreValues` init is not good yet
        this.coreValues = coreValues;
        //for now use existing tool
        this.unionFind = new HugeAtomicDisjointSetStruct(nodeCount, new Concurrency(1));

        this.edges = HugeObjectArray.newArray(Edge.class, nodeCount - 1);
        this.nodeCount = nodeCount;
    }


    @Override
    public HugeObjectArray<Edge> compute() {

        var kdRoot = kdTree.root();
        var rootId = kdRoot.id();
        while (!kdNodeSingleComponent.get(rootId)) {
            kdNodeBound.fill(Double.MAX_VALUE);
            performIteration();
        }
        return edges;
    }

    void baseCase(long p0, long p1, MutableDouble maxBound) {
        var comp0 = unionFind.setIdOf(p0);
        var comp1 = unionFind.setIdOf(p1);
        if (comp0 != comp1) {
            var arr0 = nodePropertyValues.doubleArrayValue(p0);
            var arr1 = nodePropertyValues.doubleArrayValue(p1);

            var p01Distance = Intersections.sumSquareDelta(arr0, arr1);
            var adaptedDistance = Math.max(Math.max(coreValues.get(p0), coreValues.get(p1)), p01Distance);
            if (closestDistanceTracker.tryToAssign(comp0, p0, p1, adaptedDistance)) {
                if (maxBound.doubleValue() < adaptedDistance) {
                    maxBound.setValue(adaptedDistance);
                }
            }
        }

    }

    void performIteration() {
        traversalStep(kdTree.root(), kdTree.root());
        mergeComponents();
        //reset bounds
        // TODO: find the component id to reset up to?
        closestDistanceTracker.reset(unionFind.size());
        updateSingleComponent(kdTree.root());
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

    long singleComponentOr(KdNode node, long or) {
        long id = node.id();
        if (!kdNodeSingleComponent.get(id)) return or; //this is a trick to return distinct id
        return unionFind.setIdOf(kdTree.nodeAt(node.start()));

    }

    void mergeComponents(long comp0, long comp1) {
        unionFind.union(comp0, comp1);
    }

    boolean score(KdNode kdNodeQ, KdNode kdNodeR) {
        var qId = kdNodeQ.id();

        var singleQ = singleComponentOr(kdNodeQ, -1L);
        var singleR = singleComponentOr(kdNodeR, -2L);

        if (singleQ == singleR) return false;

        if (kdNodeBound.get(qId) == Double.MAX_VALUE) return true;

        var lowerBoundQR = kdNodeQ.aabb().lowerBoundFor(kdNodeR.aabb());
        return lowerBoundQR < Math.sqrt(kdNodeBound.get(qId));

    }

    private LongStream filterNodesOnCoreValue(KdNode kdNode) {
        return kdTree.nodesContained(kdNode).filter(qPoint -> {
            var component = unionFind.setIdOf(qPoint);
            return coreValues.get(qPoint) < closestDistanceTracker.componentClosestDistance(component);
        });
    }

    void traversalStep(KdNode kdNodeQ, KdNode kdNodeR) {

        boolean score = score(kdNodeQ, kdNodeR);
        if (score) {
            var qId = kdNodeQ.id();
            if (kdNodeQ.isLeaf() && kdNodeR.isLeaf()) {
                MutableDouble newBound = new MutableDouble(Double.MIN_VALUE);
                filterNodesOnCoreValue(kdNodeQ)
                    .forEach(
                        qPoint -> {
                            filterNodesOnCoreValue(kdNodeR)
                                .forEach(rPoint -> {
                                    baseCase(qPoint, rPoint, newBound);
                                });
                        }
                    );
                if (newBound.doubleValue() != Double.MIN_VALUE) {
                    if (newBound.doubleValue() > kdNodeBound.get(qId)) {
                        kdNodeBound.set(qId, newBound.doubleValue());
                    }
                }

            } else if (kdNodeQ.isLeaf() && !kdNodeR.isLeaf()) {
                traversalStep(kdNodeQ, kdTree.leftChild(kdNodeR));
                traversalStep(kdNodeQ, kdTree.rightChild(kdNodeR));
            } else {

                traversalStep(kdTree.leftChild(kdNodeQ), kdNodeR);
                traversalStep(kdTree.rightChild(kdNodeQ), kdNodeR);

                var qBound = kdNodeBound.get(qId);
                var leftChildBound = kdNodeBound.get(kdTree.leftChild(kdNodeQ).id());
                var rightChildBound = kdNodeBound.get(kdTree.rightChild(kdNodeQ).id());
                var qCandidateBound = Math.max(leftChildBound, rightChildBound);
                if (qCandidateBound < qBound) {
                    kdNodeBound.set(qId, qCandidateBound);
                }

            }

        }

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

            this.edges.set(
                edgeCount,
                new Edge(u, v, Math.sqrt(closestDistanceTracker.componentClosestDistance(componentId)))
            );
            this.edgeCount++;

            unionFind.union(uComponent, vComponent);
        }

    }

}
