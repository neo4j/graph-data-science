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
package org.neo4j.gds.hdbscan;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@GdlExtension
class HDBScanTest {

    @GdlGraph
    private static final String DATA =
        """
            CREATE
                (a:Node {point: [1.0, 1.0]}),
                (b:Node {point: [1.0, 5.0]}),
                (c:Node {point: [1.0, 6.0]}),
                (d:Node {point: [2.0, 2.0]}),
                (e:Node {point: [8.0, 2.0]}),
                (f:Node {point: [10.0, 1.0]})
                (g:Node {point: [10.0, 2.0]})
                (h:Node {point: [12.0, 3.0]})
                (i:Node {point: [12.0, 21.0]})
            """;

    @Inject
    private TestGraph graph;

    @Test
    void shouldComputeCoresCorrectly() {

        var hdbscan = new HDBScan(
            graph, graph.nodeProperties("point"),
            new Concurrency(1),
            1,
            2,
            -1L,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var kdtree = hdbscan.buildKDTree();

        var cores = hdbscan.computeCores(kdtree, graph.nodeCount()).createCoreArray();

        assertThat(cores.toArray())
            .usingComparatorWithPrecision(1e-4)
            .containsExactlyInAnyOrder(
                16.0,        //a -  d,b  (sqrt(16)
                10.0, //b -  c,d  (sqrt(1 + 9)=sqrt(10)
                17.0,  //c -  b,d  (sqrt(1 + 16) = sqrt(17)
                10.0, //d -  a,b
                5.0,   //e - g,f (sqrt(1 + 4) = sqrt(5)
                5.0, //f - g,e
                4.0,   //g - f,e (sqrt(4)
                8.0,  //h - g,f (sqrt( 4 + 4) = sqrt(8) = 2sqrt(2)
                346.0 //i - h, c (sqrt(11^2 + 15^2) = sqrt(346)
            );

    }

    @Test
    void shouldComputeMSTWithCoreValuesCorrectly() {

        var hdbscan = new HDBScan(
            graph, graph.nodeProperties("point"),
            new Concurrency(1),
            1,
            2,
            -1,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var kdtree = hdbscan.buildKDTree();

        var result = hdbscan.dualTreeMSTPhase(kdtree, hdbscan.computeCores(kdtree, graph.nodeCount()));

        var expected = List.of(
            new Edge(graph.toMappedNodeId("i"), graph.toMappedNodeId("h"), Math.sqrt(346)),
            new Edge(graph.toMappedNodeId("a"), graph.toMappedNodeId("d"), 4.0),
            new Edge(graph.toMappedNodeId("e"), graph.toMappedNodeId("d"), 6.0),
            new Edge(graph.toMappedNodeId("e"), graph.toMappedNodeId("g"), Math.sqrt(5.0)),
            new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("c"), Math.sqrt(17.0)),
            new Edge(graph.toMappedNodeId("g"), graph.toMappedNodeId("h"), Math.sqrt(8)),
            new Edge(graph.toMappedNodeId("f"), graph.toMappedNodeId("g"), Math.sqrt(5.0)),
            new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("d"), Math.sqrt(10))
        );

        assertThat(result.edges().toArray())
            .usingElementComparator(new UndirectedEdgeComparator())
            .containsExactlyInAnyOrderElementsOf(expected);

        assertThat(result.totalDistance())
            .isEqualTo(expected.stream()
                .mapToDouble(Edge::distance)
                .sum()
            );
    }

    @Test
    void shouldComputeClusterHierarchyCorrectly() {
        HugeObjectArray<Edge> edges = HugeObjectArray.of(
            new Edge(0, 1, 5.),
            new Edge(1, 2, 3.)
        );

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(3L);

        var hdbscan = new HDBScan(
            graphMock,
            graph.nodeProperties("point"),
            new Concurrency(1),
            1,
            2,
            -1,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var dualTreeResult = new DualTreeMSTResult(edges, -1);

        var clusterHierarchy = hdbscan.createClusterHierarchy(dualTreeResult);

        assertThat(clusterHierarchy.root()).isEqualTo(4L);

        assertThat(clusterHierarchy.left(4)).isEqualTo(0);
        assertThat(clusterHierarchy.right(4)).isEqualTo(3);
        assertThat(clusterHierarchy.lambda(4)).isEqualTo(5.);
        assertThat(clusterHierarchy.size(4)).isEqualTo(3);

        assertThat(clusterHierarchy.left(3)).isEqualTo(1);
        assertThat(clusterHierarchy.right(3)).isEqualTo(2);
        assertThat(clusterHierarchy.lambda(3)).isEqualTo(3.);
        assertThat(clusterHierarchy.size(3)).isEqualTo(2);

        assertThat(clusterHierarchy.size(0)).isEqualTo(1);

    }
}
