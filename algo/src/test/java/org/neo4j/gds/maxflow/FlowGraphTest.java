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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipCursor;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class FlowGraphTest {
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
                (c)-[:R {w: 0.0}]->(b),
                (d)-[:R {w: 5.0}]->(e)
            """;

    @Inject
    private TestGraph graph;

    static FlowGraph createFlowGraph(Graph graph, long source, long target) {
        double outgoingCapacityFromSource = graph.streamRelationships(source, 0D)
            .map(RelationshipCursor::property)
            .reduce(0D, Double::sum);
        NodeWithValue[] supply = {new NodeWithValue(source, outgoingCapacityFromSource)};
        NodeWithValue[] demand = {new NodeWithValue(target, outgoingCapacityFromSource)}; //more is useless since this is max in network
        return FlowGraph.create(graph, supply, demand);
    }

    @Test
    void test() {
        var flowGraph = createFlowGraph(graph, graph.toMappedNodeId("a"), graph.toMappedNodeId("b"));

        Map<Long, Arc> map = new HashMap<>();
        Set<Long> set = new HashSet<>();
        flowGraph.forEachRelationship(
            graph.toMappedNodeId("a"), (s, t, relIdx, residualCapacity, isReverse) -> {
                set.add(relIdx);
                map.put(
                    t,
                    new Arc(residualCapacity, isReverse)
                );
                return true;
            }
        );
        assertThat(map.entrySet()).containsExactlyInAnyOrder(
            Map.entry(graph.toMappedNodeId("d"), new Arc(4D, false)),
            Map.entry(graph.toMappedNodeId("b"), new Arc(0D, true)),
            Map.entry(graph.toMappedNodeId("c"), new Arc(0D, true)),
            Map.entry(flowGraph.superSource(), new Arc(0D, true))
        );


        for (var relIdx : set) {
            flowGraph.push(relIdx, 1D, false);
            assertThat(flowGraph.flow(relIdx)).isEqualTo(1D);
        }
        map.clear();
        flowGraph.forEachRelationship(
            graph.toMappedNodeId("a"), (s, t, relIdx, residualCapacity, isReverse) -> {
                map.put(
                    t,
                    new Arc(residualCapacity, isReverse)
                );
                return true;
            }
        );
        assertThat(map.entrySet()).containsExactlyInAnyOrder(
            Map.entry(graph.toMappedNodeId("d"), new Arc(3D, false)),
            Map.entry(graph.toMappedNodeId("b"), new Arc(1D, true)),
            Map.entry(graph.toMappedNodeId("c"), new Arc(1D, true)),
            Map.entry(flowGraph.superSource(), new Arc(1D, true))
        );


        for (var relIdx : set) {
            flowGraph.push(relIdx, 1D, true);
            assertThat(flowGraph.flow(relIdx)).isEqualTo(0D);
        }
        flowGraph.forEachRelationship(
            graph.toMappedNodeId("a"), (s, t, relIdx, residualCapacity, isReverse) -> {
                map.put(
                    t,
                    new Arc(residualCapacity, isReverse)
                );
                return true;
            }
        );
        assertThat(map.entrySet()).containsExactlyInAnyOrder(
            Map.entry(graph.toMappedNodeId("d"), new Arc(4D, false)),
            Map.entry(graph.toMappedNodeId("b"), new Arc(0D, true)),
            Map.entry(graph.toMappedNodeId("c"), new Arc(0D, true)),
            Map.entry(flowGraph.superSource(), new Arc(0D, true))
        );
    }

    @Test
    void testCreateFlowResult() {
        var flowGraph = createFlowGraph(graph, graph.toMappedNodeId("b"), graph.toMappedNodeId("a"));

        flowGraph.forEachRelationship(
            graph.toMappedNodeId("a"), (s, t, relIdx, residualCapacity, isReverse) -> {
                flowGraph.push(relIdx, 2D, isReverse);
                return true;
            }
            //pushes 2 to both 'd' and superTarget.
        );

        var result = flowGraph.createFlowResult();

        assertThat(result.totalFlow).isEqualTo(2D);
        assertThat(result.flow.toArray()).containsExactlyInAnyOrder(new FlowRelationship(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("d"),
            2D
        ));
    }

    private record Arc(double residualCapacity, boolean isReverse) {
    }
}
