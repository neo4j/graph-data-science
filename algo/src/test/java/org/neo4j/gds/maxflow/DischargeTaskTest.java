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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class DischargeTaskTest {

    @GdlGraph
    private static final String GRAPH =
        """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (a)-[:R {w: 4.0}]->(d),
                (b)-[:R {w: 3.0}]->(a),
                (c)-[:R {w: 2.0}]->(a),
                (c)-[:R {w: 10.0}]->(b),
                (d)-[:R {w: 5.0}]->(e)
            """;

    @Inject
    private TestGraph graph;

    @Test
    void discharge() {
        var flowGraph = FlowGraph.create(graph);

        var excess = HugeDoubleArray.newArray(flowGraph.nodeCount());
        var label = HugeLongArray.newArray(flowGraph.nodeCount());
        var tempLabel = HugeLongArray.newArray(flowGraph.nodeCount());
        var addedExcess = HugeAtomicDoubleArray.of(flowGraph.nodeCount(), ParallelDoublePageCreator.passThrough(new Concurrency(1)));
        var isDiscovered = HugeAtomicBitSet.create(flowGraph.nodeCount());
        var workingSet = new AtomicWorkingSet(flowGraph.nodeCount());
        var beta = 12;
        var workSinceLastGR = new AtomicLong(0L);

        var targetNode = graph.toMappedNodeId("d");

        //init excess
        excess.set(graph.toMappedNodeId("c"), 10D);

        //init labels
        label.set(graph.toMappedNodeId("c"), 1); //should be raised above a, but not above b
        label.set(graph.toMappedNodeId("a"), 2);
        label.set(graph.toMappedNodeId("b"), flowGraph.nodeCount());


        workingSet.push(graph.toMappedNodeId("c"));
        var task = new DischargeTask(flowGraph.concurrentCopy(), excess, label, tempLabel, addedExcess, isDiscovered, workingSet, targetNode, beta, workSinceLastGR);

        task.discharge(graph.toMappedNodeId("c"));
        workingSet.resetIdx();
        flowGraph.forEachRelationship(graph.toMappedNodeId("c"), (s, t, relIdx, residualCapacity, isReverse) -> {
            if(t == graph.toMappedNodeId("a")) {
                assertThat(flowGraph.flow(relIdx)).isEqualTo(2D);
            }else if( t == graph.toMappedNodeId("b")) {
                assertThat(flowGraph.flow(relIdx)).isEqualTo(0D);
            }
            return true;
        });

        assertThat(label.get(graph.toMappedNodeId("a"))).isEqualTo(2L);
        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(1L);
        assertThat(excess.get(graph.toMappedNodeId("a"))).isEqualTo(0D);
        assertThat(excess.get(graph.toMappedNodeId("c"))).isEqualTo(10D);

        task.syncWorkingSet();
        workingSet.reset();

        assertThat(label.get(graph.toMappedNodeId("a"))).isEqualTo(2L);
        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(5L);
        assertThat(excess.get(graph.toMappedNodeId("a"))).isEqualTo(0D);
        assertThat(excess.get(graph.toMappedNodeId("c"))).isEqualTo(8D);

        task.updateAndSyncNewWorkingSet();

        workingSet.resetIdx();

        assertThat(label.get(graph.toMappedNodeId("a"))).isEqualTo(2L);
        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(5L);
        assertThat(excess.get(graph.toMappedNodeId("a"))).isEqualTo(2D);
        assertThat(excess.get(graph.toMappedNodeId("c"))).isEqualTo(8D);

        assertThat(workSinceLastGR.longValue()).isGreaterThan(0L);
        assertThat(workingSet.size()).isEqualTo(2);
        Set<Long> set = new HashSet<>();
        set.add(workingSet.pop());
        set.add(workingSet.pop());
        assertThat(set).containsExactlyInAnyOrder(graph.toMappedNodeId("a"), graph.toMappedNodeId("c"));
    }

}
