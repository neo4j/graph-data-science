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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@GdlExtension
class CostDischargingTest {

    @GdlGraph
    private static final String GRAPH =
        """
            CREATE
                (a:Node),
                (b:Node),
                (c:Node)
                (d:Node),
                (e1:Node),
                (e2:Node),

                (f:Node),
                (s:Node),(t:Node),
                (b)-[:R {w: 1.0, c: 10}]->(a),
                (b)-[:R {w: 8.0, c: 10}]->(c),
                (b)-[:R {w: 15.0,c: 10}]->(e2)
                (b)-[:R {w: 2.0, c: 10}]->(d),
                (b)-[:R {w: 15.0,c: 10}]->(e1)
                (b)-[:R {w: 4.0, c: 10}]->(f)

            """;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldPushAndCheckEmptied(){
        var excess = HugeDoubleArray.newArray(10);
        var prize = HugeDoubleArray.newArray(10);
        var queue = HugeLongArrayQueue.newQueue(10);
        var bitSet =new BitSet(10);
        var cost = new CostDischarging(
            mock(CostFlowGraph.class),
            excess,
            prize,
            queue,
            bitSet,
            2,
            mock(GlobalRelabelling.class),
            0,
            TerminationFlag.RUNNING_TRUE
        );
        bitSet.set(2L);

        excess.set(0,10);
        excess.set(1,0);
        excess.set(2,6);
        assertThat(cost.pushAndCheckIfEmptied(0,1,-1,5,false)).isFalse();
        assertThat(excess.get(0)).isEqualTo(5);
        assertThat(excess.get(1)).isEqualTo(5);
        assertThat(queue.remove()).isEqualTo(1L);
        assertThat(cost.pushAndCheckIfEmptied(0,2,-1,5,false)).isTrue();
        assertThat(excess.get(0)).isEqualTo(0);
        assertThat(excess.get(2)).isEqualTo(11);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void shouldSortNeighbor(){
        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        NodeWithValue[] supply = {new NodeWithValue(mappedId.of("s"), 100)};
        NodeWithValue[] demand = {new NodeWithValue(mappedId.of("t"), 100)};
        var flowGraph = new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supply,
            demand,
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        ).build();

        var excess = HugeDoubleArray.newArray(10);
        var prize = HugeDoubleArray.newArray(10);

        var cost = new CostDischarging(
            flowGraph,
            excess,
            prize,
            mock(HugeLongArrayQueue.class),
            mock(BitSet.class),
            2,
            mock(GlobalRelabelling.class),
            0,
            TerminationFlag.RUNNING_TRUE

        );

        //all edges are non-reverse:
        //costs are 10  -prize(t)
        excess.set(mappedId.of("b"),15);

        prize.set(mappedId.of("e1"),-20);
        prize.set(mappedId.of("e2"),-10);
       var k= cost.sortNeighborhood(mappedId.of("b"));

        assertThat(k).isEqualTo(4L);
        prize.set(mappedId.of("e1"),1);
        k= cost.sortNeighborhood(mappedId.of("b"));
        assertThat(k).isEqualTo(1L);

        prize.setAll( v->0);
        excess.set(mappedId.of("b"),3L);
        k= cost.sortNeighborhood(mappedId.of("b"));
        assertThat(k).isEqualTo(2L);

        prize.set(mappedId.of("f"),1);
        k= cost.sortNeighborhood(mappedId.of("b"));
        assertThat(k).isEqualTo(1L);

    }

    @Test
    void shouldDischargeCompletelyNode(){

        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        NodeWithValue[] supply = {new NodeWithValue(mappedId.of("s"), 100)};
        NodeWithValue[] demand = {new NodeWithValue(mappedId.of("t"), 100)};
        var bitset = new BitSet(100);
        var queue = HugeLongArrayQueue.newQueue(100);
        var flowGraph = new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supply,
            demand,
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        ).build();

        var excess = HugeDoubleArray.newArray(10);
        var prize = HugeDoubleArray.newArray(10);

        var cost = new CostDischarging(
            flowGraph,
            excess,
            prize,
            queue,
            bitset,
            2,
            mock(GlobalRelabelling.class),
            0,
            TerminationFlag.RUNNING_TRUE
        );

        excess.set(mappedId.of("b"),3L);
        cost.dischargeSorted(mappedId.of("b"));
        assertThat(prize.get(mappedId.of("b"))).isEqualTo(-12);
        assertThat(excess.get(mappedId.of("b"))).isEqualTo(0);
        assertThat(excess.get(mappedId.of("b"))).isEqualTo(0);
        assertThat(excess.get(mappedId.of("a"))).isEqualTo(1);
        assertThat(excess.get(mappedId.of("c"))).isEqualTo(2);
        assertThat(bitset.get(mappedId.of("a"))).isTrue();
        assertThat(bitset.get(mappedId.of("c"))).isTrue();

    }

}
