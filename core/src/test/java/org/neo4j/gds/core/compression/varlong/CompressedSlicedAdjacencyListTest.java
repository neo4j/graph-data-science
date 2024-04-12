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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.compression.common.VarLongDecoding;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class CompressedSlicedAdjacencyListTest {

    @GdlGraph
    public static final String GRAPH = "(a)" +
        "(b)-->(a)" +
        "(b)-->(c)" +
        "(c)-->(a)" +
        "(c)-->(d)" +
        "(d)-->(a)";

    @Inject
    private TestGraph graph;

    private CompressedSlicedAdjacencyList csal;

    @BeforeEach
    void setup() {
        var cal = (CompressedAdjacencyList) this.graph.relationshipTopologies()
            .get(RelationshipType.ALL_RELATIONSHIPS)
            .adjacencyList();
        this.csal = CompressedSlicedAdjacencyList.of(cal, 1);
    }

    @Test
    void degrees() {
        assertThat(csal.degree(graph.toMappedNodeId("a"))).isEqualTo(0);
        assertThat(csal.degree(graph.toMappedNodeId("b"))).isEqualTo(2);
        assertThat(csal.degree(graph.toMappedNodeId("c"))).isEqualTo(2);
        assertThat(csal.degree(graph.toMappedNodeId("d"))).isEqualTo(1);
    }

    @Test
    void offsets() {
        assertThat(csal.startOffset(graph.toMappedNodeId("a"))).isEqualTo(CompressedSlicedAdjacencyList.ZERO_DEGREE);
        assertThat(csal.endOffset(graph.toMappedNodeId("a"))).isEqualTo(CompressedSlicedAdjacencyList.ZERO_DEGREE);
        assertThat(csal.startOffset(graph.toMappedNodeId("b"))).isEqualTo(0);
        assertThat(csal.endOffset(graph.toMappedNodeId("b"))).isEqualTo(2);
        assertThat(csal.startOffset(graph.toMappedNodeId("c"))).isEqualTo(2);
        assertThat(csal.endOffset(graph.toMappedNodeId("c"))).isEqualTo(4);
        assertThat(csal.startOffset(graph.toMappedNodeId("d"))).isEqualTo(4);
        assertThat(csal.endOffset(graph.toMappedNodeId("d"))).isEqualTo(5);
    }

    @Test
    void initPageSliceZeroDegree() {
        var pageSlice = new CompressedSlicedAdjacencyList.PageSlice();
        var success = csal.initPageSlice(graph.toMappedNodeId("a"), pageSlice);
        assertThat(success).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"b,a,c", "c,a,d", "d,a"})
    void initPageSlice(String expected) {
        String[] vars = expected.split(",");
        long sourceNode = graph.toMappedNodeId(vars[0]);
        long[] expectedTargets = IntStream.range(1, vars.length).mapToLong(i -> graph.toMappedNodeId(vars[i])).toArray();

        var pageSlice = new CompressedSlicedAdjacencyList.PageSlice();
        var success = csal.initPageSlice(sourceNode, pageSlice);
        assertThat(success).isTrue();
        long[] out = new long[expectedTargets.length];
        // decompress
        VarLongDecoding.decodeDeltaVLongs(0L, pageSlice.page, pageSlice.offset, pageSlice.length, out);
        assertThat(out).isEqualTo(expectedTargets);
    }
}