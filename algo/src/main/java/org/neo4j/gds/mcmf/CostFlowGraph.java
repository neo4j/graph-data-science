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
package org.neo4j.gds.mcmf;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.maxflow.FlowGraph;
import org.neo4j.gds.maxflow.FlowRelationship;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.termination.TerminationFlag;

public final class CostFlowGraph extends FlowGraph {
    private final HugeDoubleArray cost;
    private final double totalCost;

    private CostFlowGraph(
        Graph graph,
        HugeLongArray indPtr,
        HugeDoubleArray originalCapacity,
        HugeDoubleArray flow,
        HugeLongArray reverseAdjacency,
        HugeLongArray reverseToRelIdx,
        HugeLongArray reverseIndPtr,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        HugeDoubleArray cost,
        double totalCost
    ) {
        super(graph, indPtr, originalCapacity, flow, reverseAdjacency, reverseToRelIdx, reverseIndPtr, supply, demand);
        this.cost = cost;
        this.totalCost = totalCost;
    }

    public static CostFlowGraph create(Graph graphFlow, Graph graphCost, NodeWithValue[] supply, NodeWithValue[] demand, TerminationFlag terminationFlag) {
        var superSource = graphFlow.nodeCount();
        var superTarget = graphFlow.nodeCount() + 1;
        var newNodeCount = graphFlow.nodeCount() + 2;

        var reverseDegree = HugeLongArray.newArray(newNodeCount);
        reverseDegree.setAll(x -> 0L);
        var totalCost = new MutableDouble();
        for (long nodeId = 0; nodeId < graphFlow.nodeCount(); nodeId++) {
            terminationFlag.assertRunning();
            graphFlow.forEachRelationship(
                nodeId, 0D, (s, t, capacity) -> {
                    if(capacity < 0D){
                        throw new IllegalArgumentException("Negative capacity not allowed");
                    }
                    reverseDegree.addTo(t, 1);
                    return true;
                }
            );
        }
        for (var source : supply) {
            reverseDegree.addTo(source.node(), 1);
        }
        for (var target : demand) {
            reverseDegree.addTo(target.node(), 1);
        }

        //Construct CSR ptrs.
        var indPtr = HugeLongArray.newArray(newNodeCount + 1);
        indPtr.set(0, 0);
        var reverseIndPtr = HugeLongArray.newArray(newNodeCount + 1);
        reverseIndPtr.set(0, 0);
        for (long nodeId = 0; nodeId <= superTarget; nodeId++) {
            var degree = nodeId < graphFlow.nodeCount()
                ? graphFlow.degree(nodeId)
                : (nodeId == superSource ? supply.length : demand.length);
            indPtr.set(nodeId + 1, indPtr.get(nodeId) + degree);
            reverseIndPtr.set(nodeId + 1, reverseIndPtr.get(nodeId) + reverseDegree.get(nodeId));
        }

        var newRelationshipCount = graphFlow.relationshipCount() + supply.length + demand.length;
        var originalCapacity = HugeDoubleArray.newArray(newRelationshipCount);
        var reverseToRelIdx = HugeLongArray.newArray(newRelationshipCount);
        var reverseAdjacency = HugeLongArray.newArray(newRelationshipCount);

        var flow = HugeDoubleArray.newArray(newRelationshipCount);
        flow.setAll(x -> 0D);

        //Populate CSRs
        reverseDegree.setAll(x -> 0L); //reuse
        var relIdx = new MutableLong(0L);
        RelationshipWithPropertyConsumer consumer = (s, t, capacity) -> {
            var reverseRelIdx = reverseIndPtr.get(t) + reverseDegree.get(t);
            reverseAdjacency.set(reverseRelIdx, s);
            reverseToRelIdx.set(reverseRelIdx, relIdx.longValue());
            reverseDegree.addTo(t, 1);
            originalCapacity.set(relIdx.longValue(), capacity);
            relIdx.increment();
            return true;
        };

        var cost = HugeDoubleArray.newArray(newRelationshipCount);
        cost.setAll(x -> 0D);

        //Populate CSRs
        var relIdxCost = new MutableLong(0L);
        RelationshipWithPropertyConsumer costConsumer = (s, t, relationshipCost) -> {
            cost.set(relIdxCost.longValue(), relationshipCost);
            relIdxCost.increment();
            totalCost.setValue(Math.max(totalCost.doubleValue(), relationshipCost));
            return true;
        };

        for (long nodeId = 0; nodeId < graphFlow.nodeCount(); nodeId++) {
            terminationFlag.assertRunning();
            graphFlow.forEachRelationship(nodeId, 0D, consumer);
            graphCost.forEachRelationship(nodeId, 0D, costConsumer);
        }
        for (var source : supply) {
            terminationFlag.assertRunning();
            consumer.accept(superSource, source.node(), source.value());
        }
        for (var target : demand) {
            //Fake a fully utilized (capacity) edge FROM superTarget. Flow TO superTarget can therefore be increased by capacity.
            flow.set(relIdx.longValue(), target.value());
            consumer.accept(superTarget, target.node(), target.value());
        }

        return new CostFlowGraph(
            graphFlow,
            indPtr,
            originalCapacity,
            flow,
            reverseAdjacency,
            reverseToRelIdx,
            reverseIndPtr,
            supply,
            demand,
            cost,
            totalCost.doubleValue()
        );
    }

    @Override
    public CostFlowGraph concurrentCopy() {
        return new CostFlowGraph(
            graph.concurrentCopy(),
            indPtr,
            originalCapacity,
            flow,
            reverseAdjacency,
            reverseToRelIdx,
            reverseIndPtr,
            supply,
            demand,
            cost,
            totalCost
        );
    }

    private boolean forEachOriginalRelationship(long nodeId, CostAndCapacityEdgeConsumer consumer) {
        var relIdx = new MutableLong(indPtr.get(nodeId));
        var breakEarly = new MutableBoolean(false);
        RelationshipWithPropertyConsumer consumer2 = (s, t, capacity) -> {
            var residualCapcaity = capacity - flow.get(relIdx.longValue());
            var relationshipCost = cost.get(relIdx.longValue());

            var isReverse = false;

            if(!consumer.accept(s, t, relIdx.longValue(), residualCapcaity, relationshipCost, isReverse)){
                breakEarly.setTrue();
                return false;
            }
            relIdx.increment();
            return true;
        };

        if (nodeId == superSource()) {
            for (var source : supply) {
                if (!consumer2.accept(superSource(), source.node(), source.value())){
                    return false;
                }
            }
        } else if (nodeId == superTarget()) {
            for (var target : demand) {
                if (!consumer2.accept(superTarget(), target.node(), target.value())) {
                    return false;
                }
            }
        } else {
            graph.forEachRelationship(nodeId, 0D, consumer2);
        }

        return breakEarly.isFalse();
    }

    private boolean forEachReverseRelationship(long nodeId,  CostAndCapacityEdgeConsumer consumer) {
        for (long reverseRelIdx = reverseIndPtr.get(nodeId); reverseRelIdx < reverseIndPtr.get(nodeId + 1); reverseRelIdx++) {
            var t = reverseAdjacency.get(reverseRelIdx);
            var relIdx = reverseToRelIdx.get(reverseRelIdx);
            var residualCapacity = flow.get(relIdx);
            var isReverse = true;
            if (!consumer.accept(nodeId, t, relIdx, residualCapacity, -cost.get(relIdx), isReverse)) {
                return false;
            }
        }
        return true;
    }

    public boolean forEachRelationship(long nodeId, CostAndCapacityEdgeConsumer consumer) {
        if(forEachOriginalRelationship(nodeId, consumer)) {
            return forEachReverseRelationship(nodeId,  consumer);
        }
        return false;
    }

    double maxCost() {
        return totalCost;
    }

    CostFlowResult createFlowResult() {
        var flow = HugeObjectArray.newArray(FlowRelationship.class, originalEdgeCount());
        var totalFlow = new MutableDouble(0D);
        var totalCost = new MutableDouble(0D);
//        System.out.println("CREATE RESULT");
        var idx = new MutableLong(0L);
        for (long nodeId = 0; nodeId < originalNodeCount(); nodeId++) {
            var relIdx = new MutableLong(indPtr.get(nodeId));
            graph.forEachRelationship(
                nodeId,
                0D,
                (s, t, _capacity) -> {
                    var flow_ = this.flow.get(relIdx.longValue());
                    assert(flow_ >= 0.0);

                    if (flow_ > 0.0) {
//                        System.out.println("FOO" + (cost.get(relIdx.longValue())>0));
                        var flowRelationship = new FlowRelationship(s, t, flow_);
                        flow.set(idx.getAndIncrement(), flowRelationship);
                        double operand = flow_ * cost.get(relIdx.longValue());
                  //      System.out.println("adding: " + flow_+" "+ cost.get(relIdx.longValue())+" "+operand);

                        totalCost.add(operand);

                    }
                    relIdx.increment();
                    return true;
                }
            );

        }
        //compute flow to superTarget
        forEachOriginalRelationship(
            superTarget(), (_s, _t, relIdx, _capacity, _cost, _isReverse) -> {
                //superTarget--[rel]-->target
                var fakeFlowFromSuperTarget = this.flow.get(relIdx);
                var actualFlowFromSuperTarget = fakeFlowFromSuperTarget - originalCapacity.get(relIdx);
                var actualFlowToSuperTarget = -actualFlowFromSuperTarget;
                totalFlow.add(actualFlowToSuperTarget);
                return true;
            }
        );
        return new CostFlowResult(flow.copyOf(idx.longValue()), totalFlow.doubleValue(), totalCost.doubleValue());
    }

    double maximalUnitCost() {
        var max = -200D; //fixme
        for (long r = 0; r < cost.size(); r++) {
            max = Math.max(max, cost.get(r));
        }
        return max;
    }
}
