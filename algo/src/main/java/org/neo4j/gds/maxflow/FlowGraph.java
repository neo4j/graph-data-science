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
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.atomic.AtomicLong;

public final class FlowGraph {
    private final Graph graph;
    private final HugeLongArray indPtr;
    private final HugeDoubleArray originalCapacity;
    private final HugeDoubleArray flow;
    private final HugeLongArray reverseAdjacency;
    private final HugeLongArray reverseToRelIdx;
    private final HugeLongArray reverseIndPtr;
    private final NodeWithValue[] supply;
    private final NodeWithValue[] demand;

    private FlowGraph(
        Graph graph,
        HugeLongArray indPtr,
        HugeDoubleArray originalCapacity,
        HugeDoubleArray flow,
        HugeLongArray reverseAdjacency,
        HugeLongArray reverseToRelIdx,
        HugeLongArray reverseIndPtr,
        NodeWithValue[] supply,
        NodeWithValue[] demand
    ) {
        this.graph = graph;
        this.indPtr = indPtr;
        this.originalCapacity = originalCapacity;
        this.flow = flow;
        this.reverseAdjacency = reverseAdjacency;
        this.reverseToRelIdx = reverseToRelIdx;
        this.reverseIndPtr = reverseIndPtr;
        this.supply = supply;
        this.demand = demand;
    }

    public static FlowGraph create(Graph graph, NodeWithValue[] supply, NodeWithValue[] demand, TerminationFlag terminationFlag, Concurrency concurrency) {
        var superSource = graph.nodeCount();
        var superTarget = graph.nodeCount() + 1;
        var oldNodeCount = graph.nodeCount();
        var newNodeCount = oldNodeCount + 2;

        var reverseDegree = HugeLongArray.newArray(newNodeCount);
        reverseDegree.setAll(x -> 0L);

        for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            terminationFlag.assertRunning();
            graph.forEachRelationship(
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
            var degree = nodeId < graph.nodeCount()
                ? graph.degree(nodeId)
                : (nodeId == superSource ? supply.length : demand.length);
            indPtr.set(nodeId + 1, indPtr.get(nodeId) + degree);
            reverseIndPtr.set(nodeId + 1, reverseIndPtr.get(nodeId) + reverseDegree.get(nodeId));
        }

        var newRelationshipCount = graph.relationshipCount() + supply.length + demand.length;
        var originalCapacity = HugeDoubleArray.newArray(newRelationshipCount);
        var reverseToRelIdx = HugeLongArray.newArray(newRelationshipCount);
        var reverseAdjacency = HugeLongArray.newArray(newRelationshipCount);

        var flow = HugeDoubleArray.newArray(newRelationshipCount);
        flow.setAll(x -> 0D);

        //Populate CSRs
        var cursor = HugeLongArray.newArray(newNodeCount);
        var reverseCursor = HugeAtomicLongArray.of(newNodeCount, ParalleLongPageCreator.of(concurrency, x -> 0));
        RelationshipWithPropertyConsumer consumer = (s, t, capacity) -> {
            var relIdx = indPtr.get(s) + cursor.get(s);
            cursor.addTo(s, 1);
            var reverseRelIdx = reverseIndPtr.get(t) + reverseCursor.getAndAdd(t, 1);
            reverseAdjacency.set(reverseRelIdx, s);
            reverseToRelIdx.set(reverseRelIdx, relIdx);
            originalCapacity.set(relIdx, capacity);
            return true;
        };

        var nodeId = new AtomicLong(0);

        var tasks = ParallelUtil.tasks(concurrency,
            ()->  () -> {
                var graphCopy = graph.concurrentCopy();
                long v;
                while ((v = nodeId.getAndIncrement()) < oldNodeCount) {
                    graphCopy.forEachRelationship(v, 0D, consumer);
                }
            }
        );

        RunWithConcurrency.builder()
            .tasks(tasks)
            .concurrency(concurrency)
            .run();


        for (var source : supply) {
            terminationFlag.assertRunning();
            consumer.accept(superSource, source.node(), source.value());
        }
        var r = newRelationshipCount - demand.length;
        for (var target : demand) {
            //Fake a fully utilized (capacity) edge FROM superTarget. Flow TO superTarget can therefore be increased by capacity.
            flow.set(r++, target.value());
            consumer.accept(superTarget, target.node(), target.value());
        }

        return new FlowGraph(
            graph,
            indPtr,
            originalCapacity,
            flow,
            reverseAdjacency,
            reverseToRelIdx,
            reverseIndPtr,
            supply,
            demand
        );
    }

    public FlowGraph concurrentCopy() {
        return new FlowGraph(
            graph.concurrentCopy(),
            indPtr,
            originalCapacity,
            flow,
            reverseAdjacency,
            reverseToRelIdx,
            reverseIndPtr,
            supply,
            demand
        );
    }

    private boolean forEachOriginalRelationship(long nodeId, ResidualEdgeConsumer consumer) {
        var earlyTermination = new MutableBoolean(false);
        var relIdx = new MutableLong(indPtr.get(nodeId));
        RelationshipWithPropertyConsumer originalConsumer = (s, t, capacity) -> {
            var residualCapacity = capacity - flow.get(relIdx.longValue());
            var isReverse = false;
            if(!consumer.accept(s, t, relIdx.getAndIncrement(), residualCapacity, isReverse)) {
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
            graph.forEachRelationship(nodeId, 0D, originalConsumer);
        }
        return earlyTermination.get();
    }

    void forEachReverseRelationship(long nodeId, ResidualEdgeConsumer consumer) {
        for (long reverseRelIdx = reverseIndPtr.get(nodeId); reverseRelIdx < reverseIndPtr.get(nodeId + 1); reverseRelIdx++) {
            var t = reverseAdjacency.get(reverseRelIdx);
            var relIdx = reverseToRelIdx.get(reverseRelIdx);
            var residualCapacity = flow.get(relIdx);
            var isReverse = true;
            if(!consumer.accept(nodeId, t, relIdx, residualCapacity, isReverse)) {
                break;
            }
        }
    }

    public void forEachRelationship(long nodeId, ResidualEdgeConsumer consumer) {
        if(!forEachOriginalRelationship(nodeId, consumer)){
            forEachReverseRelationship(nodeId, consumer);
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

    private long originalEdgeCount() {
        return graph.relationshipCount();
    }

    long edgeCount() {
        return graph.relationshipCount() + supply.length + demand.length;
    }

    long originalNodeCount() {
        return graph.nodeCount();
    }

    public long nodeCount() {
        return graph.nodeCount() + 2;
    }

    long outDegree(long nodeId) {
        return nodeId < originalNodeCount() ? graph.degree(nodeId) : (nodeId == originalNodeCount() ? supply.length : 0);
    }

    long inDegree(long nodeId) {
        return reverseIndPtr.get(nodeId) + reverseIndPtr.get(nodeId + 1);
    }

    long degree(long nodeId) {
        return inDegree(nodeId) + outDegree(nodeId);
    }

    double residualCapacity(long relIdx, boolean isReverse) {
        if (!isReverse) {
            return flow.get(relIdx);
        }
        return originalCapacity.get(relIdx) - flow.get(relIdx);
    }

    long superSource() {
        return graph.nodeCount();
    }

    long superTarget() {
        return graph.nodeCount() + 1;
    }

    FlowResult createFlowResult() {
        var flow = HugeObjectArray.newArray(FlowRelationship.class, originalEdgeCount());
        var totalFlow = new MutableDouble(0D);

        var idx = new MutableLong(0L);
        for (long nodeId = 0; nodeId < originalNodeCount(); nodeId++) {
            var relIdx = new MutableLong(indPtr.get(nodeId));
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
