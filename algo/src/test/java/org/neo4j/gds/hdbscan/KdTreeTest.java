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
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class KdTreeTest {

    @GdlGraph
    private static final String DATA =
        """
                (a:Node { point: [2.0, 3.0]}),
                (b:Node { point: [5.0, 4.0]}),
                (c:Node { point: [9.0, 6.0]}),
                (d:Node { point: [4.0, 7.0]}),
                (e:Node { point: [8.0, 1.0]}),
                (f:Node { point: [7.0, 2.0]})
            """;

    @Inject
    private TestGraph graph;


    @Test
    void shouldFindNeighbours() {

        var points = graph.nodeProperties("point");
        var kdTree = new KdTreeBuilder(graph, points, 1, 1).build();
        var queryPoint = new double[]{9d, 2d};
        var neighbours = kdTree.neighbours(queryPoint, 2);
        assertThat(neighbours)
            .isNotNull()
            .hasSize(2)
            .containsExactlyInAnyOrder(
                new Neighbour(
                    graph.toMappedNodeId("e"),
                    1.4142135623730951
                ),
                new Neighbour(
                    graph.toMappedNodeId("f"),
                    2.0
                )
            );
    }

    @Test
    void shouldNotFindItself() {

        var points = graph.nodeProperties("point");
        var kdTree = new KdTreeBuilder(graph, points, 1, 1).build();
        var neighbours = kdTree.neighbours(graph.toMappedNodeId("a"), 2);
        assertThat(neighbours)
            .isNotNull()
            .hasSize(2)
            .containsExactlyInAnyOrder(
                new Neighbour(
                    graph.toMappedNodeId("d"),
                    Math.sqrt(20)
                ),
                new Neighbour(
                    graph.toMappedNodeId("b"),
                    Math.sqrt(10)
                )

            );
    }

    @Test
    void shouldCorrectlyFigureOutDescendants() {
        var root = KdNode.createLeaf(0, 10, 100, null);
        var l1 = KdNode.createLeaf(1, 10, 50, null);
        var l2 = KdNode.createLeaf(1, 20, 50, null);
        var l3 = KdNode.createLeaf(1, 100, 150, null);

        var kdtree = new KdTree(null, null, root, 3);

        assertThat(kdtree.descentOfOther(root, l1)).isTrue();
        assertThat(kdtree.descentOfOther(l1, root)).isTrue();

        assertThat(kdtree.descentOfOther(root, l2)).isTrue();
        assertThat(kdtree.descentOfOther(l2, root)).isTrue();

        assertThat(kdtree.descentOfOther(root, l3)).isFalse();
        assertThat(kdtree.descentOfOther(l3, root)).isFalse();


    }

}
