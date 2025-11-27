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

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.relationships.RelationshipCursor;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.maxflow.NodeWithValue;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class CostFlowGraphTest {
    @GdlGraph
    private static final String GRAPH =
        """
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (a)-[:R {w: 4.0, c: 1}]->(d),
                (b)-[:R {w: 3.0, c: 4}]->(a),
                (c)-[:R {w: 2.0, c: 2}]->(a),
                (c)-[:R {w: 0.0, c: 10}]->(b),
                (d)-[:R {w: 5.0, c: 9}]->(e)
            """;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    static CostFlowGraph createCostFlowGraph(Graph graphOfFlows, Graph graphOfCosts, long source, long target) {
        double outgoingCapacityFromSource = graphOfFlows.streamRelationships(source, 0D)
            .map(RelationshipCursor::property)
            .reduce(0D, Double::sum);
        NodeWithValue[] supply = {new NodeWithValue(source, outgoingCapacityFromSource)};
        NodeWithValue[] demand = {new NodeWithValue(target, outgoingCapacityFromSource)}; //more is useless since this is max in network
        return new CostFlowGraphBuilder(graphOfFlows, graphOfCosts, supply, demand, TerminationFlag.RUNNING_TRUE, new Concurrency(1)).build();
    }

    @Test
    void test() {
        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        var costFlowGraph = createCostFlowGraph(graphOfFlows, graphOfCosts, mappedId.of("a"), mappedId.of("b"));
        var prize = HugeDoubleArray.newArray(costFlowGraph.nodeCount());
        prize.setAll(x -> 0D);

        prize.set(mappedId.of("a"), -1);
        prize.set(mappedId.of("b"), -3);
        prize.set(mappedId.of("c"), -2);
        prize.set(mappedId.of("d"), -5);
        prize.set(mappedId.of("e"), -10);

        Map<Long, CostArc> map = new HashMap<>();
        Set<Long> set = new HashSet<>();


        costFlowGraph.forEachRelationship(
            mappedId.of("a"),
            (s, t, relIdx, residualCapacity, cost, isReverse) -> {
                set.add(relIdx);
                var reducedCost = prize.get(s) + cost - prize.get(t);
                map.put(
                    t,
                    new CostArc(residualCapacity, reducedCost, isReverse)
                );
                return true;
            }
        );

        assertThat(map.entrySet()).containsExactlyInAnyOrder(
            Map.entry(mappedId.of("d"), new CostArc(4D, 1D + (-1D) - (-5D), false)),
            Map.entry(mappedId.of("b"), new CostArc(0D, -4D + (-1D) - (-3D), true)),
            Map.entry(mappedId.of("c"), new CostArc(0D, -2D + (-1D) - (-2D), true)),
            Map.entry(costFlowGraph.superSource(), new CostArc(0D, -0 + (-1D) - (0D), true))
        );

        assertThat(costFlowGraph.maximalUnitCost()).isEqualTo(10D);
    }

    @Test
    void testBreakIteration() {

        var graphOfFlows = graphStore.getGraph("w");
        var graphOfCosts = graphStore.getGraph("c");
        IdFunction mappedId = name -> graphStore.nodes().toMappedNodeId(idFunction.of(name));

        var costFlowGraph = createCostFlowGraph(graphOfFlows, graphOfCosts, mappedId.of("a"), mappedId.of("b"));

        var idx = new MutableInt(0);
        costFlowGraph.forEachRelationship(
            mappedId.of("a"), (s, t, r, residualCapacity, __,isReverse) -> idx.incrementAndGet() < 1
        );
        assertThat(idx.intValue()).isEqualTo(1);

        costFlowGraph.forEachReverseRelationship(
            mappedId.of("a"), (s, t, r, residualCapacity, __,isReverse) -> idx.incrementAndGet() < 2
        );
        assertThat(idx.intValue()).isEqualTo(2);
    }

    private record CostArc(double residualCapacity, double reducedCost, boolean isReverse) { }
    }
