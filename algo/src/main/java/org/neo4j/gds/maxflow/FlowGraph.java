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
package org.neo4j.gds.maxflow;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;

public class FlowGraph {
    protected final Graph graph;
    protected final HugeLongArray outRelationshipIndexOffset;
    protected final HugeDoubleArray originalCapacity;
    protected final HugeDoubleArray flow;
    protected final HugeLongArray reverseAdjacency;
    protected final HugeLongArray reverseRelationshipMap;
    protected final HugeLongArray reverseRelationshipIndexOffset;
    protected final NodeWithValue[] supply;
    protected final NodeWithValue[] demand;
    private final NodeConstraintsIdMap nodeCapacities;

    protected FlowGraph(
        Graph graph,
        HugeLongArray outRelationshipIndexOffset,
        HugeDoubleArray originalCapacity,
        HugeDoubleArray flow,
        HugeLongArray reverseAdjacency,
        HugeLongArray reverseRelationshipMap,
        HugeLongArray reverseRelationshipIndexOffset,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        NodeConstraintsIdMap nodeCapacities
    ) {
        this.graph = graph;
        this.outRelationshipIndexOffset = outRelationshipIndexOffset;
        this.originalCapacity = originalCapacity;
        this.flow = flow;
        this.reverseAdjacency = reverseAdjacency;
        this.reverseRelationshipMap = reverseRelationshipMap;
        this.reverseRelationshipIndexOffset = reverseRelationshipIndexOffset;
        this.supply = supply;
        this.demand = demand;
        this.nodeCapacities = nodeCapacities;
    }


    protected FlowGraph(
        Graph graph,
        HugeLongArray outRelationshipIndexOffset,
        HugeDoubleArray originalCapacity,
        HugeDoubleArray flow,
        HugeLongArray reverseAdjacency,
        HugeLongArray reverseRelationshipMap,
        HugeLongArray reverseRelationshipIndexOffset,
        NodeWithValue[] supply,
        NodeWithValue[] demand
    ) {
        this(
            graph,
            outRelationshipIndexOffset,
            originalCapacity,
            flow,
            reverseAdjacency,
            reverseRelationshipMap,
            reverseRelationshipIndexOffset,
            supply,
            demand,
            new NodeConstraintsIdMap.IgnoreNodeConstraints()
        );
    }

    public FlowGraph concurrentCopy() {
        return new FlowGraph(
            graph.concurrentCopy(),
            outRelationshipIndexOffset,
            originalCapacity,
            flow,
            reverseAdjacency,
            reverseRelationshipMap,
            reverseRelationshipIndexOffset,
            supply,
            demand,
            nodeCapacities
        );
    }

    protected boolean forEachOriginalRelationship(long nodeId, ResidualEdgeConsumer consumer) {
        var earlyTermination = new MutableBoolean(false);
        var relIdx = new MutableLong(outRelationshipIndexOffset.get(nodeId));
        RelationshipWithPropertyConsumer originalConsumer = (s, t, capacity) -> {
            var residualCapacity = capacity - flow.get(relIdx.longValue());
            var isReverse = false;
            if(!consumer.accept(s, nodeCapacities.mapNode(t), relIdx.getAndIncrement(), residualCapacity, isReverse)) {
                earlyTermination.setTrue();
                return false;
            }
            return true;
        };
        if (nodeId == superSource()) {
            for (var source : supply) {
                if(!originalConsumer.accept(superSource(), source.node(), source.value())){
                    break;
                }
            }
        } else if (nodeId == superTarget()) {
            for (var target : demand) {
                if(!originalConsumer.accept(superTarget(), target.node(), target.value())){
                    break;
                }
            }
        } else {
            if (nodeCapacities.isFakeNode(nodeId)) {
                var realNode = nodeCapacities.realNodeOf(nodeId);
                var relId = nodeCapacities.capacityRelId(realNode);
                var capacity = originalCapacity.get(relId);
               return consumer.accept(nodeId,realNode,relId,capacity-flow.get(relId),false);
            }else {
                graph.forEachRelationship(nodeId, 0D, originalConsumer);
            }
        }
        return !earlyTermination.get();
    }

   protected boolean forEachReverseRelationship(long nodeId, ResidualEdgeConsumer consumer) {
       if (nodeCapacities.hasCapacityConstraint(nodeId)){
            long capacityNode = nodeCapacities.toFakeNodeOf(nodeId);
            long relIdx = nodeCapacities.capacityRelId(nodeId);
            return consumer.accept(nodeId,capacityNode,relIdx,flow.get(relIdx),true);
        }else {
            for (long reverseRelIdx = reverseRelationshipIndexOffset.get(nodeId); reverseRelIdx < reverseRelationshipIndexOffset.get(nodeId + 1); reverseRelIdx++) {
                var t = reverseAdjacency.get(reverseRelIdx);
                var relIdx = reverseRelationshipMap.get(reverseRelIdx);
                var residualCapacity = flow.get(relIdx);
                if (!consumer.accept(nodeId,t,relIdx,residualCapacity,true)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean forEachRelationship(long nodeId, ResidualEdgeConsumer consumer) {
        if(forEachOriginalRelationship(nodeId, consumer)) {
            return forEachReverseRelationship(nodeId, consumer);
        } else {
            return false;
        }
    }

    public double flow(long relIdx) {
        return flow.get(relIdx);
    }

    public void push(long relIdx, double delta, boolean isReverse) {
        //(s)-[rel]->(t)
        if (isReverse) {
            flow.addTo(relIdx, -delta);
        } else {
            flow.addTo(relIdx, delta);
        }
    }

    protected long originalEdgeCount() {
        return graph.relationshipCount();
    }

    public long edgeCount() {
        return graph.relationshipCount() + supply.length + demand.length + nodeCapacities.numberOfCapacityNodes();
    }

    protected long originalNodeCount() {
        return graph.nodeCount();
    }

    public long nodeCount() {
        return graph.nodeCount() + 2 + nodeCapacities.numberOfCapacityNodes();
    }

    public long outDegree(long nodeId) {
        if (nodeCapacities.isFakeNode(nodeId)){
            return 1;
        }
        return nodeId < originalNodeCount() ? graph.degree(nodeId) : (nodeId == originalNodeCount() ? supply.length : 0);
    }

    long inDegree(long nodeId) {
        if (nodeId < graph.nodeCount() && nodeCapacities.hasCapacityConstraint(nodeId)){
            return 1;
        }
        return reverseRelationshipIndexOffset.get(nodeId + 1) - reverseRelationshipIndexOffset.get(nodeId);
    }

    public long degree(long nodeId) {
        return inDegree(nodeId) + outDegree(nodeId);
    }

    public double reverseResidualCapacity(long relIdx, boolean isReverse) {
        if (!isReverse) {
            return flow.get(relIdx);
        }
        return originalCapacity.get(relIdx) - flow.get(relIdx);
    }

    public long superSource() {
        return graph.nodeCount();
    }

    protected long superTarget() {
        return graph.nodeCount() + 1;
    }

    FlowResult createFlowResult() {
        var flow = HugeObjectArray.newArray(FlowRelationship.class, originalEdgeCount());
        var totalFlow = new MutableDouble(0D);

        var idx = new MutableLong(0L);
        for (long nodeId = 0; nodeId < originalNodeCount(); nodeId++) {
            var relIdx = new MutableLong(outRelationshipIndexOffset.get(nodeId));
            graph.forEachRelationship(
                nodeId,
                0D,
                (s, t, _capacity) -> {
                    var flow_ = this.flow.get(relIdx.longValue());
                    if (flow_ > 0.0) {
                        var flowRelationship = new FlowRelationship(s, t, flow_);
                        flow.set(idx.getAndIncrement(), flowRelationship);
                    }
                    relIdx.increment();
                    return true;
                }
            );

        }
        //compute flow to superTarget
        forEachOriginalRelationship(
            superTarget(), (_s, _t, relIdx, _capacity, _isReverse) -> {
                //superTarget--[rel]-->target
                var fakeFlowFromSuperTarget = this.flow.get(relIdx);
                var actualFlowFromSuperTarget = fakeFlowFromSuperTarget - originalCapacity.get(relIdx);
                var actualFlowToSuperTarget = -actualFlowFromSuperTarget;
                totalFlow.add(actualFlowToSuperTarget);
                return true;
            }
        );
        return new FlowResult(flow.copyOf(idx.longValue()), totalFlow.doubleValue());
    }
}
