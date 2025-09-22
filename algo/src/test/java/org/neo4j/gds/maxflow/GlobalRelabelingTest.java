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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GlobalRelabelingTest {
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

    //d = 0
    //a = 1
    //b,c = 2,
    //c <- nodeCount(source) = 5
    //e <- nodeCount(init) = 5

    @Test
    void test() {
        var flowGraph = FlowGraph.create(graph);

        var label = HugeLongArray.newArray(flowGraph.nodeCount());
        label.setAll((i) -> flowGraph.nodeCount());
        GlobalRelabeling.globalRelabeling(
            flowGraph,
            label,
            graph.toMappedNodeId("c"),
            graph.toMappedNodeId("d"),
            new Concurrency(1)
        );

        assertThat(label.get(graph.toMappedNodeId("a"))).isEqualTo(1L);
        assertThat(label.get(graph.toMappedNodeId("b"))).isEqualTo(2L);
        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(5L);
        assertThat(label.get(graph.toMappedNodeId("d"))).isEqualTo(0L);
        assertThat(label.get(graph.toMappedNodeId("e"))).isEqualTo(5L);
    }

    @Test
    void test2() {
        var flowGraph = FlowGraph.create(graph);

        var label = HugeLongArray.newArray(flowGraph.nodeCount());
        label.setAll((i) -> flowGraph.nodeCount());
        GlobalRelabeling.globalRelabeling(
            flowGraph,
            label,
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("e"),
            new Concurrency(1)
        );

        assertThat(label.get(graph.toMappedNodeId("a"))).isEqualTo(5L);
        assertThat(label.get(graph.toMappedNodeId("b"))).isEqualTo(5L);
        assertThat(label.get(graph.toMappedNodeId("c"))).isEqualTo(5L);
        assertThat(label.get(graph.toMappedNodeId("d"))).isEqualTo(1L);
        assertThat(label.get(graph.toMappedNodeId("e"))).isEqualTo(0L);
    }
}
