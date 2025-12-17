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
    protected  HugeLongArray outRelationshipIndexOffset;
    protected  HugeDoubleArray originalCapacity;
    protected  HugeDoubleArray flow;
    protected  HugeLongArray reverseRelationshipIndexOffset;
    protected  HugeLongArray reverseRelationshipMap;
    protected  HugeLongArray reverseAdjacency;
    protected final NodeConstraintsIdMap nodeConstraintsIdMap;

    public FlowGraphBuilder(
        Graph capacityGraph,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        TerminationFlag terminationFlag,
        Concurrency concurrency,
        NodeConstraintsIdMap nodeConstraintsIdMap
    ) {
        this.capacityGraph = capacityGraph;
        this.supply = supply;
        this.demand = demand;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
        this.nodeConstraintsIdMap = nodeConstraintsIdMap;
    }

    public FlowGraphBuilder(
        Graph capacityGraph,
        NodeWithValue[] supply,
        NodeWithValue[] demand,
        TerminationFlag terminationFlag,
        Concurrency concurrency
    ) {
         this(
            capacityGraph,
            supply,
            demand,
            terminationFlag,
            concurrency,
            new NodeConstraintsIdMap.IgnoreNodeConstraints()
        );
    }

    protected void setUpCapacities(){
        var superSource = capacityGraph.nodeCount();
        var superTarget = capacityGraph.nodeCount() + 1;
        var oldNodeCount = capacityGraph.nodeCount();
        var newNodeCount = oldNodeCount + 2 + nodeConstraintsIdMap.numberOfCapacityNodes();

        var reverseDegree = HugeLongArray.newArray(newNodeCount);
        reverseDegree.setAll(x -> 0L);

        for (long nodeId = 0; nodeId < capacityGraph.nodeCount(); nodeId++) {
            terminationFlag.assertRunning();
            capacityGraph.forEachRelationship(
                nodeId, 0D, (s, t, capacity) -> {
                    if (capacity < 0D) {
                        throw new IllegalArgumentException("Negative capacity not allowed");
                    }
                    reverseDegree.addTo(nodeConstraintsIdMap.mapNode(t), 1);
                    return true;
                }
            );
        }
        for (var source : supply) {
            reverseDegree.addTo(nodeConstraintsIdMap.mapNode(source.node()), 1);
        }
        for (var target : demand) {
            reverseDegree.addTo(nodeConstraintsIdMap.mapNode(target.node()), 1);
        }

        //Construct CSR ptrs.
        outRelationshipIndexOffset = HugeLongArray.newArray(newNodeCount + 1);
        outRelationshipIndexOffset.set(0, 0);
        reverseRelationshipIndexOffset = HugeLongArray.newArray(newNodeCount + 1);
        reverseRelationshipIndexOffset.set(0, 0);
        for (long nodeId = 0; nodeId < newNodeCount; nodeId++) {
            int outDegree;
            if (nodeId < capacityGraph.nodeCount()) { //handling normal nodes
                outDegree = capacityGraph.degree(nodeId);
            }else if (nodeId <=superTarget){
                //handling superSource, and superTarget nodes
                outDegree = (nodeId == superSource ? supply.length : demand.length);
            }else{
                outDegree = 0; //we handle the fake nodes outside of arrays, and get their value externally
                //(at least for the moment)
            }
            outRelationshipIndexOffset.set(nodeId + 1, outRelationshipIndexOffset.get(nodeId) + outDegree);
            reverseRelationshipIndexOffset.set(nodeId + 1, reverseRelationshipIndexOffset.get(nodeId) + reverseDegree.get(nodeId));
        }

        var newRelationshipCount = capacityGraph.relationshipCount()
            + supply.length
            + demand.length
            + nodeConstraintsIdMap.numberOfCapacityNodes();

        originalCapacity = HugeDoubleArray.newArray(newRelationshipCount- nodeConstraintsIdMap.numberOfCapacityNodes());
        reverseRelationshipMap = HugeLongArray.newArray(newRelationshipCount);
        reverseAdjacency = HugeLongArray.newArray(newRelationshipCount);

        flow = HugeDoubleArray.newArray(newRelationshipCount);
        flow.setAll(x -> 0D);

        //Populate CSRs
        var cursor = HugeLongArray.newArray(newNodeCount);
        var reverseCursor = HugeAtomicLongArray.of(newNodeCount, ParalleLongPageCreator.of(concurrency, x -> 0));

        RelationshipWithPropertyConsumer consumer = (s, t, capacity) -> {
            var relIdx = outRelationshipIndexOffset.get(s) + cursor.get(s);
            cursor.addTo(s, 1);

            var tMapped = nodeConstraintsIdMap.mapNode(t);
            var reverseRelIdx = reverseRelationshipIndexOffset.get(tMapped) + reverseCursor.getAndAdd(tMapped, 1);
            reverseAdjacency.set(reverseRelIdx, s);
            reverseRelationshipMap.set(reverseRelIdx, relIdx);
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

        var r = newRelationshipCount - demand.length - nodeConstraintsIdMap.numberOfCapacityNodes();
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
              outRelationshipIndexOffset,
              new DefaultRelationships(originalCapacity,flow,nodeConstraintsIdMap),
              reverseAdjacency,
              reverseRelationshipMap,
              reverseRelationshipIndexOffset,
              supply,
              demand,
              nodeConstraintsIdMap
          );
      }

}
