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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class SequentialDischargingTest {

    @GdlGraph
    private static final String GRAPH =
        """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (c)-[:R {w: 2.0}]-> (a),
                (c)-[:R {w: 20.0}]->(b),
                (c)-[:R {w: 10.0}]->(d)
            """;

    @Inject
    private TestGraph graph;

    @Test
    void dischargeShouldRaiseLabelsToFulfillDemand() {
        var targetNode = graph.toMappedNodeId("d");
        var  source  = new NodeWithValue(graph.toMappedNodeId("e"),100);
        var  target  = new NodeWithValue(targetNode,100);

        var flowGraph = FlowGraph.create(
            graph,
            new NodeWithValue[]{source},
            new NodeWithValue[]{target},
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        );

        var excess = HugeDoubleArray.newArray(flowGraph.nodeCount());
        var label = HugeLongArray.newArray(flowGraph.nodeCount());
        var workingQueue = HugeLongArrayQueue.newQueue(flowGraph.nodeCount());
        var inWorkingQueue = new BitSet(flowGraph.nodeCount());
        var seqDischarging = new SequentialDischarging(
            flowGraph,
            excess,
            label,
            workingQueue,
            inWorkingQueue,
            null,
            new GapDetector.Noop(),
            1000,
            0,
            0,
            ProgressTracker.NULL_TRACKER
        );


        //init excess
        excess.set(graph.toMappedNodeId("c"), 10D);

        //init labels
        label.set(graph.toMappedNodeId("c"), 1);
        label.set(graph.toMappedNodeId("a"), 2); //should be raised above a, but not above b
        label.set(graph.toMappedNodeId("b"), flowGraph.nodeCount());
        label.set(graph.toMappedNodeId("d"), 3); //should be raised above d


        seqDischarging.discharge(graph.toMappedNodeId("c"));
        flowGraph.forEachRelationship(graph.toMappedNodeId("c"), (s, t, relIdx, residualCapacity, isReverse) -> {
            if(t == graph.toMappedNodeId("a")) {
                assertThat(flowGraph.flow(relIdx)).isEqualTo(2D);
            }else if(t == graph.toMappedNodeId("b")) {
                assertThat(flowGraph.flow(relIdx)).isEqualTo(0D);
            }else if (t== graph.toMappedNodeId("d")){
                assertThat(flowGraph.flow(relIdx)).isEqualTo(8D);
            }
            return true;
        });

        assertThat(label.get(graph.toMappedNodeId("a"))).isEqualTo(2L);
        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(4L);
        assertThat(label.get(graph.toMappedNodeId("d"))).isEqualTo(3L);

        assertThat(excess.get(graph.toMappedNodeId("a"))).isEqualTo(2D);
        assertThat(excess.get(graph.toMappedNodeId("c"))).isEqualTo(0D);
        assertThat(excess.get(graph.toMappedNodeId("d"))).isEqualTo(8D);

        assertThat(workingQueue.size()).isEqualTo(2L);
        Set<Long> set = new HashSet<>();
        set.add(workingQueue.remove());
        set.add(workingQueue.remove());
        assertThat(set).containsExactlyInAnyOrder(graph.toMappedNodeId("d"), graph.toMappedNodeId("a"));
    }

    @Test
    void dischargeShouldNotAlwaysNeedToRaiseLabels() {
        var targetNode = graph.toMappedNodeId("d");
        var  source  = new NodeWithValue(graph.toMappedNodeId("e"),100);
        var  target  = new NodeWithValue(targetNode,100);

        var flowGraph = FlowGraph.create(
            graph,
            new NodeWithValue[]{source},
            new NodeWithValue[]{target},
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        );

        var excess = HugeDoubleArray.newArray(flowGraph.nodeCount());
        var label = HugeLongArray.newArray(flowGraph.nodeCount());
        var workingQueue = HugeLongArrayQueue.newQueue(flowGraph.nodeCount());
        var inWorkingQueue = new BitSet(flowGraph.nodeCount());
        var seqDischarging = new SequentialDischarging(
            flowGraph,
            excess,
            label,
            workingQueue,
            inWorkingQueue,
            null,
            new GapDetector.Noop(),
            1000,
            0,
            0,
            ProgressTracker.NULL_TRACKER
        );


        //init excess
        excess.set(graph.toMappedNodeId("c"), 2D);

        //init labels
        label.set(graph.toMappedNodeId("c"), 2);
        label.set(graph.toMappedNodeId("a"), flowGraph.nodeCount());
        label.set(graph.toMappedNodeId("b"), 3);
        label.set(graph.toMappedNodeId("d"), 1); //should work here


        seqDischarging.discharge(graph.toMappedNodeId("c"));
        flowGraph.forEachRelationship(graph.toMappedNodeId("c"), (s, t, relIdx, residualCapacity, isReverse) -> {
             if (t== graph.toMappedNodeId("d")){
                assertThat(flowGraph.flow(relIdx)).isEqualTo(2D);
            }else{
                 assertThat(flowGraph.flow(relIdx)).isEqualTo(0D);

             }
            return true;
        });

        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(2L);
        assertThat(label.get(graph.toMappedNodeId("b"))).isEqualTo(3L);
        assertThat(label.get(graph.toMappedNodeId("d"))).isEqualTo(1L);

        assertThat(excess.get(graph.toMappedNodeId("c"))).isEqualTo(0D);
        assertThat(excess.get(graph.toMappedNodeId("b"))).isEqualTo(0D);
        assertThat(excess.get(graph.toMappedNodeId("d"))).isEqualTo(2D);

        assertThat(workingQueue.size()).isEqualTo(1L);
        assertThat(workingQueue.peek()).isEqualTo(graph.toMappedNodeId("d"));
    }

}
