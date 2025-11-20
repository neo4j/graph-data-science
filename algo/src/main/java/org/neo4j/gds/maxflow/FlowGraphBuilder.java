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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.atomic.AtomicLong;

public class FlowGraphBuilder {

    protected final Graph capacityGraph;
    protected final NodeWithValue[] supply;
    protected final NodeWithValue[] demand;
    protected final TerminationFlag terminationFlag;
    protected final Concurrency concurrency;
    protected  HugeLongArray indPtr;
    protected  HugeDoubleArray originalCapacity;
    protected  HugeDoubleArray flow;
    protected  HugeLongArray reverseIndPtr;
    protected  HugeLongArray reverseToRelIdx;
    protected  HugeLongArray reverseAdjacency;

    public FlowGraphBuilder(
        Graph capacityGraph,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        TerminationFlag terminationFlag,
        Concurrency concurrency
    ) {
        this.capacityGraph = capacityGraph;
        this.supply = supply;
        this.demand = demand;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
    }

    protected void setUpCapacities(){
        var superSource = capacityGraph.nodeCount();
        var superTarget = capacityGraph.nodeCount() + 1;
        var oldNodeCount = capacityGraph.nodeCount();
        var newNodeCount = oldNodeCount + 2;

        var reverseDegree = HugeLongArray.newArray(newNodeCount);
        reverseDegree.setAll(x -> 0L);

        for (long nodeId = 0; nodeId < capacityGraph.nodeCount(); nodeId++) {
            terminationFlag.assertRunning();
            capacityGraph.forEachRelationship(
                nodeId, 0D, (s, t, capacity) -> {
                    if (capacity < 0D) {
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
        indPtr = HugeLongArray.newArray(newNodeCount + 1);
        indPtr.set(0, 0);
        reverseIndPtr = HugeLongArray.newArray(newNodeCount + 1);
        reverseIndPtr.set(0, 0);
        for (long nodeId = 0; nodeId <= superTarget; nodeId++) {
            var degree = nodeId < capacityGraph.nodeCount()
                ? capacityGraph.degree(nodeId)
                : (nodeId == superSource ? supply.length : demand.length);
            indPtr.set(nodeId + 1, indPtr.get(nodeId) + degree);
            reverseIndPtr.set(nodeId + 1, reverseIndPtr.get(nodeId) + reverseDegree.get(nodeId));
        }

        var newRelationshipCount = capacityGraph.relationshipCount() + supply.length + demand.length;
        originalCapacity = HugeDoubleArray.newArray(newRelationshipCount);
        reverseToRelIdx = HugeLongArray.newArray(newRelationshipCount);
        reverseAdjacency = HugeLongArray.newArray(newRelationshipCount);

        flow = HugeDoubleArray.newArray(newRelationshipCount);
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

        var tasks = ParallelUtil.tasks(
            concurrency,
            () -> () -> {
                var capacityGraphCopy = capacityGraph.concurrentCopy();
                long v;
                while ((v = nodeId.getAndIncrement()) < oldNodeCount) {
                    capacityGraphCopy.forEachRelationship(v, 0D, consumer);
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
    }
      public FlowGraph build() {

        setUpCapacities();
        return new FlowGraph(
              capacityGraph,
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


}
