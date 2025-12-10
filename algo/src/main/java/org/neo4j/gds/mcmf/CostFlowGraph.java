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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.maxflow.FlowGraph;
import org.neo4j.gds.maxflow.FlowRelationship;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.maxflow.ResidualEdgeConsumer;

public final class CostFlowGraph extends FlowGraph {
    private final HugeDoubleArray cost;

    CostFlowGraph(
        Graph graph,
        HugeLongArray indPtr,
        HugeDoubleArray originalCapacity,
        HugeDoubleArray flow,
        HugeLongArray reverseAdjacency,
        HugeLongArray reverseToRelIdx,
        HugeLongArray reverseIndPtr,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        HugeDoubleArray cost
    ) {
        super(graph, indPtr, originalCapacity, flow, reverseAdjacency, reverseToRelIdx, reverseIndPtr, supply, demand);
        this.cost = cost;
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
            cost
        );
    }

    private boolean forEachOriginalRelationship(long nodeId, CostAndCapacityEdgeConsumer consumer) {
        ResidualEdgeConsumer adaptedConsumer = (s,t,relIdx,residualCapacity,isReverse)-> consumer.accept(
            s,
            t,
            relIdx,
            residualCapacity,
            cost.get(relIdx),
            isReverse
        );
        return  forEachOriginalRelationship(nodeId,adaptedConsumer);

    }

    boolean forEachReverseRelationship(long nodeId, CostAndCapacityEdgeConsumer consumer) {
       ResidualEdgeConsumer adaptedConsumer = (s,t,relIdx,residualCapacity,isReverse)-> consumer.accept(
            s,
            t,
            relIdx,
            residualCapacity,
            -cost.get(relIdx),
            isReverse
       );

       return forEachReverseRelationship(nodeId,adaptedConsumer);

    }

    public boolean forEachRelationship(long nodeId, CostAndCapacityEdgeConsumer consumer) {
        if(forEachOriginalRelationship(nodeId, consumer)) {
            return forEachReverseRelationship(nodeId,  consumer);
        }
        return false;
    }

    CostFlowResult createFlowResult() {
        var flow = HugeObjectArray.newArray(FlowRelationship.class, originalEdgeCount());
        var totalFlow = new MutableDouble(0D);
        var totalCost = new MutableDouble(0D);
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
                        var flowRelationship = new FlowRelationship(s, t, flow_);
                        flow.set(idx.getAndIncrement(), flowRelationship);
                        double operand = flow_ * cost.get(relIdx.longValue());
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
                var fakeFlowFromSuperTarget = this.flow.get(relIdx);
                var actualFlowFromSuperTarget = fakeFlowFromSuperTarget - originalCapacity.get(relIdx);
                var actualFlowToSuperTarget = -actualFlowFromSuperTarget;
                totalFlow.add(actualFlowToSuperTarget);
                return true;
            }
        );
        return new CostFlowResult(new FlowResult(flow.copyOf(idx.longValue()), totalFlow.doubleValue()), totalCost.doubleValue());
    }

    double maximalUnitCost() {
        var max = Double.NEGATIVE_INFINITY; //fixme
        for (long r = 0; r < cost.size(); r++) {
            max = Math.max(max, cost.get(r));
        }
        return max;
    }

    long maxInPlusOutDegree() {
        var max = 0L;
        for (long v = 0; v < nodeCount(); v++) {
            max = Math.max(max, this.degree(v));
        }
        return max;
    }
}
