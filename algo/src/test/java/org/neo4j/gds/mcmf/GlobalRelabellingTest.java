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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@GdlExtension
class GlobalRelabellingTest {
    @GdlGraph
    private static final String GRAPH =
        """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (g:Node {id: 4}),
                (h:Node {id: 5}),
                (s:Node{id: 4}),(t:Node{id:4}),
                (a)-[:R {w: 4.0, c: 1}]->(b),
                (g)-[:R {w: 4.0, c: -20}]->(a),
                (h)-[:R {w: 4.0, c: -200}]->(a),
                (g)-[:R {w: 4.0, c: 1}]->(b)
            """;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;
    @Test
    void shouldComputeCorrectlyTimestamps(){
        var globalRelabelling = new GlobalRelabelling(mock(CostFlowGraph.class),null,null);
        //40 / 10 = 4 + 5 = 9
        assertThat(globalRelabelling.computeEventTime(100,10,50,10,5)).isEqualTo(9);
        //40 / 12 = 3.something = 4 + 5 = 9
        assertThat(globalRelabelling.computeEventTime(100,10,50,12,5)).isEqualTo(9);
    }

    @Test
    void shouldDoOperationsCorrectly(){
        //this tests traverse hand-by-hand
        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        NodeWithValue[] supply = {new NodeWithValue(mappedId.of("s"), 100)};
        NodeWithValue[] demand = {new NodeWithValue(mappedId.of("t"), 100)};
        var flowGraph = new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supply,demand,
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        ).withNegativeCosts().build();

        var prize = HugeDoubleArray.newArray(20);
        prize.set(mappedId.of("a"),20);
        prize.set(mappedId.of("g"),100);

        var excess = HugeDoubleArray.newArray(20);

        excess.set(mappedId.of("g"),10);
        excess.set(mappedId.of("h"),10);

        var globalRelabelling =new GlobalRelabelling(flowGraph,excess,prize);
        //at this moment, prize(a) = 20 - 2*5 = 10 , prize(g) = 100-2*5 = 90
        //and reverse(b->a) = -(-1) + prize(a) - prize(b) = +1 + 10 -0 = 11
        //and reverse(b->g) = -(-1) + prize(g) - prize(b) = +1 + 90 = 91
        MutableLong activeNodesNotFound = new MutableLong(100);
        globalRelabelling.traverseNode(mappedId.of("b"),5,2, activeNodesNotFound);
        //should be 11/2 = 6 + 5 =11
        assertThat(globalRelabelling.extractFromPriorityQueue(2,activeNodesNotFound)).isEqualTo(11);
        //prize of a now should become : 20 - 11*2 = -2
        //prize of g now should become: 100 -11*2 = 78 (but not commited)
        //prize of h now should become: 0 -11*2 = -22
        assertThat(prize.get(mappedId.of("a"))).isEqualTo(-2);
        assertThat(prize.get(mappedId.of("g"))).isEqualTo(100);

        assertThat(activeNodesNotFound.get()).isEqualTo(100L);

        //reverse(g->a) = -(20) + prize(g) - prize(a) = -20 + 78 + 2 = 60
        //reverse(h->a) = -(200) + prize(h) - prize(a) = -200 -22 + 2  <0
        globalRelabelling.traverseNode(mappedId.of("a"),11,2,activeNodesNotFound);
        assertThat(prize.get(mappedId.of("h"))).isEqualTo(-22);
        assertThat(activeNodesNotFound.get()).isEqualTo(99L);

        assertThat(globalRelabelling.extractFromPriorityQueue(2,activeNodesNotFound)).isEqualTo(41);
        assertThat(prize.get(mappedId.of("g"))).isEqualTo(18);
        assertThat(activeNodesNotFound.get()).isEqualTo(98L);
    }

    @Test
    void shouldAddAnExhaustFrontier() {
        //this test repeats computations from above except called by another function
        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        NodeWithValue[] supply = {new NodeWithValue(mappedId.of("s"), 100)};
        NodeWithValue[] demand = {new NodeWithValue(mappedId.of("t"), 100)};
        var flowGraph = new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supply,demand,
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        ).withNegativeCosts().build();

        var prize = HugeDoubleArray.newArray(20);
        prize.set(mappedId.of("a"), 20);
        prize.set(mappedId.of("g"), 100);

        var excess = HugeDoubleArray.newArray(20);

        excess.set(mappedId.of("g"), 10);
        excess.set(mappedId.of("h"), 10);

        var globalRelabelling = new GlobalRelabelling(flowGraph, excess, prize);

        globalRelabelling.addToFrontier(mappedId.of("b"));
        MutableLong activeNodesNotFound = new MutableLong(100);

        globalRelabelling.exhaustFrontier(11,2,activeNodesNotFound);

        assertThat(prize.get(mappedId.of("a"))).isEqualTo(-2);
        assertThat(prize.get(mappedId.of("h"))).isEqualTo(-22);
        assertThat(prize.get(mappedId.of("g"))).isEqualTo(100);
        assertThat(activeNodesNotFound.get()).isEqualTo(99L);

        assertThat(globalRelabelling.extractFromPriorityQueue(2,activeNodesNotFound)).isEqualTo(41);
        assertThat(prize.get(mappedId.of("g"))).isEqualTo(18);
        assertThat(activeNodesNotFound.get()).isEqualTo(98L);

        }

    @Test
    void shouldRunEndToEnd() {
        //this test tests that everything works end to end
        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        NodeWithValue[] supply = {new NodeWithValue(mappedId.of("s"), 100)};
        NodeWithValue[] demand = {new NodeWithValue(mappedId.of("t"), 100)};
        var flowGraph = new CostFlowGraphBuilder(
            graphOfFlows,
            graphOfCosts,
            supply,demand,
            TerminationFlag.RUNNING_TRUE,
            new Concurrency(1)
        ).withNegativeCosts().build();

        var prize = HugeDoubleArray.newArray(20);
        prize.set(mappedId.of("a"), 20);
        prize.set(mappedId.of("g"), 100);

        var excess = HugeDoubleArray.newArray(20);

        excess.set(mappedId.of("g"), 10);
        excess.set(mappedId.of("b"),-100);
        var globalRelabelling = new GlobalRelabelling(flowGraph, excess, prize);

        globalRelabelling.relabellingWithPriorityQueue(2);

        assertThat(prize.get(mappedId.of("a"))).isEqualTo(-2);
        assertThat(prize.get(mappedId.of("h"))).isEqualTo(-22);
        assertThat(prize.get(mappedId.of("g"))).isEqualTo(18);
        assertThat(prize.get(mappedId.of("s"))).isEqualTo(-82);

    }

        @Test
        void shouldEarlyBreakIfAllActiveNodesAreFound(){
            var graphOfFlows = graphStore.getGraph("w");
            var graphOfCosts = graphStore.getGraph("c");
            IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

            NodeWithValue[] supply = {new NodeWithValue(mappedId.of("s"), 100)};
            NodeWithValue[] demand = {new NodeWithValue(mappedId.of("t"), 100)};
            var flowGraph = new CostFlowGraphBuilder(
                graphOfFlows,
                graphOfCosts,
                supply,demand,
                TerminationFlag.RUNNING_TRUE,
                new Concurrency(1)
            ).withNegativeCosts().build();

            var prize = HugeDoubleArray.newArray(20);
            prize.set(mappedId.of("a"), 20);
            prize.set(mappedId.of("g"), 100);

            var excess = HugeDoubleArray.newArray(20);

            excess.set(mappedId.of("a"), 10);

            var globalRelabelling = new GlobalRelabelling(flowGraph, excess, prize);

            globalRelabelling.addToFrontier(mappedId.of("b"));
            MutableLong activeNodesNotFound = new MutableLong(1);

            globalRelabelling.exhaustFrontier(11,2,activeNodesNotFound);

            assertThat(prize.get(mappedId.of("a"))).isEqualTo(-2);
            assertThat(prize.get(mappedId.of("h"))).isEqualTo(0);


        }

    }
