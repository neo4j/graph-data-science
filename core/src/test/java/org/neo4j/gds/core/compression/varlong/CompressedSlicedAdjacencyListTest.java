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
import org.neo4j.gds.compression.common.VarLongDecoding;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.compression.common.BumpAllocator.PAGE_SIZE;
import static org.neo4j.gds.core.compression.varlong.CompressedSlicedAdjacencyList.ZERO_DEGREE;

@GdlExtension
class CompressedSlicedAdjacencyListTest {

    @GdlGraph
    public static final String GRAPH = "(a)" +
        "(b)-->(a)" +
        "(b)-->(c)" +
        "(c)-->(a)" +
        "(c)-->(d)" +
        "(d)-->(a)";

    @GdlGraph(graphNamePrefix = "gap")
    public static final String GAP_GRAPH = "(a)" +
        "(b)" +
        "(c)" +
        "(d)" +
        "(e)" +
        "(a)-->(c)" +
        "(a)-->(d)" +
        "(d)-->(e)";

    @Inject
    private TestGraph graph;

    @Inject
    private TestGraph gapGraph;

    private CompressedSlicedAdjacencyList csal;

    private CompressedSlicedAdjacencyList gapCsal;

    @BeforeEach
    void setup() {
        var cal = (CompressedAdjacencyList) this.graph.relationshipTopologies()
            .get(RelationshipType.ALL_RELATIONSHIPS)
            .adjacencyList();
        this.csal = CompressedSlicedAdjacencyList.of(cal, new Concurrency(1));
        var gapCal = (CompressedAdjacencyList) this.gapGraph.relationshipTopologies()
            .get(RelationshipType.ALL_RELATIONSHIPS)
            .adjacencyList();
        this.gapCsal = CompressedSlicedAdjacencyList.of(gapCal, new Concurrency(1));
    }

    @Test
    void gapDegrees() {
        assertThat(gapCsal.degree(gapGraph.toMappedNodeId("a"))).isEqualTo(2);
        assertThat(gapCsal.degree(gapGraph.toMappedNodeId("b"))).isEqualTo(0);
        assertThat(gapCsal.degree(gapGraph.toMappedNodeId("c"))).isEqualTo(0);
        assertThat(gapCsal.degree(gapGraph.toMappedNodeId("d"))).isEqualTo(1);
        assertThat(gapCsal.degree(gapGraph.toMappedNodeId("e"))).isEqualTo(0);
    }

    @Test
    void gapOffsets() {
        assertThat(gapCsal.startOffset(gapGraph.toMappedNodeId("a"))).isEqualTo(0);
        assertThat(gapCsal.endOffset(gapGraph.toMappedNodeId("a"))).isEqualTo(2);
        assertThat(gapCsal.startOffset(gapGraph.toMappedNodeId("b"))).isEqualTo(ZERO_DEGREE);
        assertThat(gapCsal.endOffset(gapGraph.toMappedNodeId("b"))).isEqualTo(ZERO_DEGREE);
        assertThat(gapCsal.startOffset(gapGraph.toMappedNodeId("c"))).isEqualTo(ZERO_DEGREE);
        assertThat(gapCsal.endOffset(gapGraph.toMappedNodeId("c"))).isEqualTo(ZERO_DEGREE);
        assertThat(gapCsal.startOffset(gapGraph.toMappedNodeId("d"))).isEqualTo(2);
        assertThat(gapCsal.endOffset(gapGraph.toMappedNodeId("d"))).isEqualTo(PAGE_SIZE);
        assertThat(gapCsal.startOffset(gapGraph.toMappedNodeId("e"))).isEqualTo(ZERO_DEGREE);
        assertThat(gapCsal.endOffset(gapGraph.toMappedNodeId("e"))).isEqualTo(ZERO_DEGREE);
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
        assertThat(csal.startOffset(graph.toMappedNodeId("a"))).isEqualTo(ZERO_DEGREE);
        assertThat(csal.endOffset(graph.toMappedNodeId("a"))).isEqualTo(ZERO_DEGREE);
        assertThat(csal.startOffset(graph.toMappedNodeId("b"))).isEqualTo(0);
        assertThat(csal.endOffset(graph.toMappedNodeId("b"))).isEqualTo(2);
        assertThat(csal.startOffset(graph.toMappedNodeId("c"))).isEqualTo(2);
        assertThat(csal.endOffset(graph.toMappedNodeId("c"))).isEqualTo(4);
        assertThat(csal.startOffset(graph.toMappedNodeId("d"))).isEqualTo(4);
        assertThat(csal.endOffset(graph.toMappedNodeId("d"))).isEqualTo(PAGE_SIZE);
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
        long[] expectedTargets = IntStream.range(1, vars.length)
            .mapToLong(i -> graph.toMappedNodeId(vars[i]))
            .toArray();

        var pageSlice = new CompressedSlicedAdjacencyList.PageSlice();
        var success = csal.initPageSlice(sourceNode, pageSlice);
        assertThat(success).isTrue();
        long[] out = new long[expectedTargets.length];
        // decompress
        VarLongDecoding.decodeDeltaVLongs(0L, pageSlice.page, pageSlice.offset, pageSlice.length, out);
        assertThat(out).isEqualTo(expectedTargets);
    }
}