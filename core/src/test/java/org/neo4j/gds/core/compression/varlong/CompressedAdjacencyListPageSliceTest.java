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
package org.neo4j.gds.core.compression.varlong;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.GdlSupport;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestGraph;
import org.neo4j.gds.compression.common.VarLongDecoding;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

class CompressedAdjacencyListPageSliceTest {

    private record Graph(
        TestGraph testGraph,
        Map<String, Integer> degrees,
        String[] adjacencies,
        Map<String, Optional<Slice>> slices
    ) {
        CompressedAdjacencyList compressedAdjacencyList() {
            return (CompressedAdjacencyList) testGraph.relationshipTopologies()
                .get(RelationshipType.ALL_RELATIONSHIPS)
                .adjacencyList();
        }
    }

    private record Slice(int offset, int length) {
    }

    private static final String GRAPH = "(a)" +
        "(b)-->(a)" +
        "(b)-->(c)" +
        "(c)-->(a)" +
        "(c)-->(d)" +
        "(d)-->(a)";
    private static final Map<String, Integer> GRAPH_DEGREES = Map.of(
        "a", 0,
        "b", 2,
        "c", 2,
        "d", 1
    );
    private static final String[] GRAPH_ADJACENCIES = {"b,a,c", "c,a,d", "d,a"};
    private static final Map<String, Optional<Slice>> GRAPH_SLICES = Map.of(
        "a", Optional.empty(),
        "b", Optional.of(new Slice(0, 2)),
        "c", Optional.of(new Slice(2, 2)),
        "d", Optional.of(new Slice(4, 1))
    );

    private static final String GAP_GRAPH = "(a)" +
        "(b)" +
        "(c)" +
        "(d)" +
        "(e)" +
        "(a)-->(c)" +
        "(a)-->(d)" +
        "(d)-->(e)";
    private static final Map<String, Integer> GAP_GRAPH_DEGREES = Map.of(
        "a", 2,
        "b", 0,
        "c", 0,
        "d", 1,
        "e", 0
    );
    private static final String[] GAP_GRAPH_ADJACENCIES = {"a,c,d", "d,e"};
    private static final Map<String, Optional<Slice>> GAP_GRAPH_SLICES = Map.of(
        "a", Optional.of(new Slice(0, 2)),
        "b", Optional.empty(),
        "c", Optional.empty(),
        "d", Optional.of(new Slice(2, 1)),
        "e", Optional.empty()
    );

    private static Stream<Arguments> graphs() {
        return Stream.of(
            new Graph(
                GdlSupport.fromGdl(GRAPH),
                GRAPH_DEGREES,
                GRAPH_ADJACENCIES,
                GRAPH_SLICES
            ),
            new Graph(
                GdlSupport.fromGdl(GAP_GRAPH),
                GAP_GRAPH_DEGREES,
                GAP_GRAPH_ADJACENCIES,
                GAP_GRAPH_SLICES
            )
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("graphs")
    void degrees(Graph graph) {
        var g = graph.testGraph();
        var csal = graph.compressedAdjacencyList();

        graph.degrees().forEach((node, degree) -> {
            assertThat(csal.degree(g.toMappedNodeId(node))).isEqualTo(degree);
        });
    }

    @ParameterizedTest
    @MethodSource("graphs")
    void slices(Graph graph) {
        var g = graph.testGraph();
        var csal = graph.compressedAdjacencyList();
        var pageSlice = csal.newPageSlice();
        graph.slices().forEach((node, expectedSlice) -> {
            if (expectedSlice.isEmpty()) {
                assertFalse(csal.initPageSlice(g.toMappedNodeId(node), pageSlice));
            } else {
                var success = csal.initPageSlice(g.toMappedNodeId(node), pageSlice);
                assertThat(success).isTrue();
                assertThat(pageSlice.page).isNotNull();
                assertThat(pageSlice.offset).isEqualTo(expectedSlice.get().offset());
                assertThat(pageSlice.length).isEqualTo(expectedSlice.get().length());
            }
        });
    }

    @ParameterizedTest
    @MethodSource("graphs")
    void decode(Graph graph) {
        var g = graph.testGraph();
        var cal = graph.compressedAdjacencyList();
        Arrays.stream(graph.adjacencies()).forEach(expected -> assertDecoded(g, cal, expected));
    }

    static void assertDecoded(TestGraph graph, CompressedAdjacencyList cal, String expected) {
        String[] vars = expected.split(",");
        long sourceNode = graph.toMappedNodeId(vars[0]);
        long[] expectedTargets = IntStream.range(1, vars.length)
            .mapToLong(i -> graph.toMappedNodeId(vars[i]))
            .toArray();

        var pageSlice = cal.newPageSlice();
        var success = cal.initPageSlice(sourceNode, pageSlice);
        assertThat(success).isTrue();
        long[] out = new long[expectedTargets.length];
        // decompress
        VarLongDecoding.decodeDeltaVLongs(0L, pageSlice.page, pageSlice.offset, pageSlice.length, out);
        assertThat(out).isEqualTo(expectedTargets);
    }
}
