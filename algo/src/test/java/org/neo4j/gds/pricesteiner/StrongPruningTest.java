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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;


@GdlExtension
class StrongPruningTest {

    @GdlExtension
    @Nested
    class FirstGraph {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a1:node)," +
                "  (a2:node)," +
                "  (a3:node)," +
                "  (a4:node)," +
                "  (a5:node)," +
                "  (a6:node)," +
                "  (a7:node)," +
                "  (a8:node)," +
                "  (a9:node)," +

                "(a2)-[:R{w:0}]->(a1)," +
                "(a1)-[:R{w:100}]->(a3)," +
                "(a2)-[:R{w:8}]->(a4)," +
                "(a2)-[:R{w:100}]->(a5)," +
                "(a4)-[:R{w:3}]->(a6)," +
                "(a4)-[:R{w:4}]->(a7)," +
                "(a5)-[:R{w:100}]->(a8)," +
                "(a5)-[:R{w:100}]->(a9)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldMarkInvalidNodesCorrectly() {
            var bitSet = new BitSet(graph.nodeCount());
            for (int i = 0; i < graph.nodeCount(); ++i) {
                bitSet.set(i);
            }
            var strongPruning = new StrongPruning(new TreeStructure(graph, null, graph.nodeCount()), bitSet, null);

            var a1 = graph.toMappedNodeId("a1");
            var a2 = graph.toMappedNodeId("a2");
            var a3 = graph.toMappedNodeId("a3");
            var a4 = graph.toMappedNodeId("a4");
            var a6 = graph.toMappedNodeId("a6");
            var a7 = graph.toMappedNodeId("a7");

            strongPruning.setNodesAsInvalid(a4, HugeLongArray.newArray(graph.nodeCount()), a2);

            assertThat(bitSet.cardinality()).isEqualTo(6);
            assertThat(bitSet.get(a4)).isFalse();
            assertThat(bitSet.get(a6)).isFalse();
            assertThat(bitSet.get(a7)).isFalse();

            strongPruning.setNodesAsInvalid(a2, HugeLongArray.newArray(graph.nodeCount()), a1);
            assertThat(bitSet.cardinality()).isEqualTo(2);
            assertThat(bitSet.get(a1)).isTrue();
            assertThat(bitSet.get(a3)).isTrue();

            strongPruning.setNodesAsInvalid(a3, HugeLongArray.newArray(graph.nodeCount()), a1);
            assertThat(bitSet.cardinality()).isEqualTo(1);
            assertThat(bitSet.get(a1)).isTrue();
        }

        @Test
        void shouldApplyDynamicProgrammingProperly() {
            var bitSet = new BitSet(graph.nodeCount());
            for (int i = 0; i < graph.nodeCount(); ++i) {
                bitSet.set(i);
            }

            HugeLongArray degrees = HugeLongArray.newArray(graph.nodeCount());
            degrees.setAll(v -> graph.degree(v));

            HugeDoubleArray prizes = HugeDoubleArray.newArray(graph.nodeCount());
            for (long u = 1; u <= 9; ++u) {
                prizes.set(graph.toMappedNodeId("a" + u), u);
            }

            var strongPruning = new StrongPruning(
                new TreeStructure(graph, degrees, graph.nodeCount()),
                bitSet,
                prizes::get
            );

            strongPruning.performPruning();

            assertThat(bitSet.cardinality()).isEqualTo(5);
            assertThat(bitSet.get(graph.toMappedNodeId("a1"))).isTrue();
            assertThat(bitSet.get(graph.toMappedNodeId("a2"))).isTrue();
            assertThat(bitSet.get(graph.toMappedNodeId("a4"))).isTrue();
            assertThat(bitSet.get(graph.toMappedNodeId("a6"))).isTrue();
            assertThat(bitSet.get(graph.toMappedNodeId("a7"))).isTrue();

            var resultTree = strongPruning.resultTree();
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a1"))).isEqualTo(graph.toMappedNodeId("a2"));
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a4"))).isEqualTo(graph.toMappedNodeId("a2"));
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a6"))).isEqualTo(graph.toMappedNodeId("a4"));
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a7"))).isEqualTo(graph.toMappedNodeId("a4"));
            assertThat(resultTree.relationshipToParentCost().get(graph.toMappedNodeId("a1"))).isEqualTo(0);
            assertThat(resultTree.relationshipToParentCost().get(graph.toMappedNodeId("a4"))).isEqualTo(8);
            assertThat(resultTree.relationshipToParentCost().get(graph.toMappedNodeId("a6"))).isEqualTo(3);
            assertThat(resultTree.relationshipToParentCost().get(graph.toMappedNodeId("a7"))).isEqualTo(4);

            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a5"))).isEqualTo(PriceSteinerTreeResult.PRUNED);
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a8"))).isEqualTo(PriceSteinerTreeResult.PRUNED);
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a9"))).isEqualTo(PriceSteinerTreeResult.PRUNED);
            assertThat(resultTree.parentArray().get(graph.toMappedNodeId("a3"))).isEqualTo(PriceSteinerTreeResult.PRUNED);

        }
    }

    @GdlExtension
    @Nested
    class GraphWithTwoChildren {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a1:node)," +
                "  (a2:node)," +
                "  (a3:node)," +
                "(a1)-[:R{w:100}]->(a2)," +
                "(a1)-[:R{w:100}]->(a3)";


        @Inject
        private TestGraph graph;

        @ParameterizedTest
        @MethodSource("input")
        void shouldApplyDynamicProgramming(double prizeOf2, double prizeOf3, long[] expected) {

            var bitSet = new BitSet(graph.nodeCount());
            for (int i = 0; i < graph.nodeCount(); ++i) {
                bitSet.set(i);
            }

            HugeLongArray degrees = HugeLongArray.newArray(graph.nodeCount());
            degrees.setAll(v -> graph.degree(v));

            HugeDoubleArray prizes = HugeDoubleArray.newArray(graph.nodeCount());
            prizes.set(graph.toMappedNodeId("a2"), prizeOf2);
            prizes.set(graph.toMappedNodeId("a3"), prizeOf3);
            var strongPruning = new StrongPruning(
                new TreeStructure(graph, degrees, graph.nodeCount()),
                bitSet,
                prizes::get
            );
            strongPruning.performPruning();

            assertThat(Arrays.stream(expected)
                .map(v -> graph.toMappedNodeId("a" + v))
                .filter(bitSet::get)
                .count()
            ).isEqualTo(expected.length);
        }

        static Stream<Arguments> input() {
            return Stream.of(
                arguments(1, 1, new long[]{1L}),
                arguments(105, 105, new long[]{1L, 2L, 3L}),
                arguments(50, 105, new long[]{1L, 3L}),
                arguments(105, 50, new long[]{1L, 2L})
            );
        }
    }


}
