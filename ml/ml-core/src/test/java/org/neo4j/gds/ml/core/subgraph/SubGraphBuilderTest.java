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
package org.neo4j.gds.ml.core.subgraph;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.core.NeighborhoodFunction;
import org.neo4j.gds.ml.core.RelationshipWeights;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class SubGraphBuilderTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 0)
    private static final String GRAPH =
        "(a), (b), (c), (d), (e), (f), (g), (h), (i), " +
        "(a)-[]->(b)," +
        "(a)-[]->(d)," +
        "(a)-[]->(e)," +
        "(b)-[]->(c)," +
        "(b)-[]->(d)," +
        "(c)-[]->(f)," +
        "(d)-[]->(g)," +
        "(e)-[]->(g)," +
        "(e)-[]->(h)," +
        "(e)-[]->(i)";

    @Inject
    private IdFunction idFunction;

    @Inject
    private Graph graph;

    private NeighborhoodFunction neighborhoodFunction;

    @BeforeEach
    void setup() {
        neighborhoodFunction = (nodeId) -> new NeighborhoodSampler(0L).sample(graph, nodeId, 100);
    }

    @Test
    void shouldBuildSubGraphSingleNode() {
        SubGraph subGraph = SubGraph.buildSubGraph(new long[]{0L}, neighborhoodFunction, RelationshipWeights.UNWEIGHTED);

        int[] expectedAdj = new int[] {1, 2, 3};
        long[] expectedNeighbors = new long[] {
            idFunction.of("a"),
            idFunction.of("b"),
            idFunction.of("d"),
            idFunction.of("e")
        };

        assertEquals(1, subGraph.batchSize());
        assertArrayEquals(expectedAdj, subGraph.neighbors(0));
        assertArrayEquals(expectedNeighbors, subGraph.originalNodeIds());
    }

    @Test
    void shouldBuildSubGraphAnotherNode() {
        SubGraph subGraph = SubGraph.buildSubGraph(new long[]{1L}, neighborhoodFunction, RelationshipWeights.UNWEIGHTED);

        int[] expectedAdj = new int[] {1, 2, 3};
        long[] expectedNeighbors = new long[] {
            idFunction.of("b"),
            idFunction.of("a"),
            idFunction.of("c"),
            idFunction.of("d")
        };

        assertEquals(1, subGraph.batchSize());
        assertArrayEquals(expectedAdj, subGraph.neighbors(0));
        assertArrayEquals(expectedNeighbors, subGraph.originalNodeIds());
    }

    @Test
    void shouldBuildSubGraphMultipleNodes() {
        SubGraph subGraph = SubGraph.buildSubGraph(new long[]{0L, 1L, 2L}, neighborhoodFunction, RelationshipWeights.UNWEIGHTED);

        assertThat(subGraph.batchIds()).containsExactly(0, 1, 2);

        // start a,b,c  : 0, 1, 2
        // neighbors d,e,f : 3,4,5
        // adjA : 1 3 4
        // adjB : 0 2 3
        // adjC : 1 5
        int[] expectedAdjA = new int[] {1, 3, 4};
        int[] expectedAdjB = new int[] {0, 2, 3};
        int[] expectedAdjC = new int[] {1, 5};

        long[] expectedNeighbors = new long[]{
            idFunction.of("a"), idFunction.of("b"), idFunction.of("c"),
            idFunction.of("d"), idFunction.of("e"), idFunction.of("f")
        };

        assertEquals(3, subGraph.batchSize());
        assertArrayEquals(expectedAdjA, subGraph.neighbors(0));
        assertArrayEquals(expectedAdjB, subGraph.neighbors(1));
        assertArrayEquals(expectedAdjC, subGraph.neighbors(2));

        assertArrayEquals(expectedNeighbors, subGraph.originalNodeIds());
    }

    @Test
    void shouldBuildMultipleSubGraphs() {
        List<SubGraph> subGraphs = SubGraph.buildSubGraphs(
            new long[]{0L, 1L, 2L},
            List.of(neighborhoodFunction, neighborhoodFunction),
            RelationshipWeights.UNWEIGHTED
        );

        int[] expectedAdjA = new int[] {1, 3, 4};
        int[] expectedAdjB = new int[] {0, 2, 3};
        int[] expectedAdjC = new int[] {1, 5};

        long[] expectedNeighbors = new long[]{
            idFunction.of("a"), idFunction.of("b"),
            idFunction.of("c"), idFunction.of("d"),
            idFunction.of("e"), idFunction.of("f")
        };

        assertEquals(3, subGraphs.get(0).batchSize());
        assertArrayEquals(expectedAdjA, subGraphs.get(0).neighbors(0));
        assertArrayEquals(expectedAdjB, subGraphs.get(0).neighbors(1));
        assertArrayEquals(expectedAdjC, subGraphs.get(0).neighbors(2));
        assertArrayEquals(expectedNeighbors, subGraphs.get(0).originalNodeIds());

        // start a,b,c,d,e,f  : 0, 1, 2, 3,4,5
        // neighbors g,h,i : 6,7,8
        // adjA : 1 3 4
        // adjB : 0 2 3
        // adjC : 1 5
        // adjD : 0 1
        // adjE : 0 6 7 8
        // adjF : 2

        int[] expectedAdj2A = new int[] {1, 3, 4};
        int[] expectedAdj2B = new int[] {0, 2, 3};
        int[] expectedAdj2C = new int[] {1, 5};
        int[] expectedAdj2D = new int[] {0, 1, 6};
        int[] expectedAdj2E = new int[] {0, 6, 7, 8};
        int[] expectedAdj2F = new int[] {2};

        expectedNeighbors = LongStream.concat(
            Arrays.stream(expectedNeighbors),
            LongStream.of(idFunction.of("g"), idFunction.of("h"), idFunction.of("i"))
        ).toArray();

        assertEquals(6, subGraphs.get(1).batchSize());
        assertArrayEquals(expectedAdj2A, subGraphs.get(1).neighbors(0));
        assertArrayEquals(expectedAdj2B, subGraphs.get(1).neighbors(1));
        assertArrayEquals(expectedAdj2C, subGraphs.get(1).neighbors(2));
        assertArrayEquals(expectedAdj2D, subGraphs.get(1).neighbors(3));
        assertArrayEquals(expectedAdj2E, subGraphs.get(1).neighbors(4));
        assertArrayEquals(expectedAdj2F, subGraphs.get(1).neighbors(5));
        assertArrayEquals(expectedNeighbors, subGraphs.get(1).originalNodeIds());
    }

    @Test
    void shouldHandleDuplicatedNodes() {
        SubGraph subGraph = SubGraph.buildSubGraph(
            new long[]{
                idFunction.of("a"),
                idFunction.of("b"),
                idFunction.of("i"),
                idFunction.of("i"),
                idFunction.of("a"),
                idFunction.of("a")
            },
            neighborhoodFunction,
            RelationshipWeights.UNWEIGHTED
        );

        assertEquals(3, subGraph.neighbors.length);
    }

    @Test
    void shouldHandleNodeFilteredGraphs() {
        GdlFactory factory = GdlFactory.of("(a:IGNORE), (b:N), (c:N), (d:N), (a)-->(d), (b)-->(c), (c)-->(d), (d)-->(b)");
        IdFunction idFunction = factory::nodeId;
        CSRGraphStore graphStore = factory.build();
        Graph filteredGraph = graphStore.getGraph("N", RelationshipType.ALL_RELATIONSHIPS.name, Optional.empty());

        long[] batch = {
            filteredGraph.toMappedNodeId(idFunction.of("b")),
            filteredGraph.toMappedNodeId(idFunction.of("d"))
        };
        var subgraph = SubGraph.buildSubGraph(
            batch,
            (nodeId) -> filteredGraph.streamRelationships(nodeId, -1D).mapToLong(RelationshipCursor::targetId),
            RelationshipWeights.UNWEIGHTED
        );

        // b, c, d
        assertThat(subgraph.nodeCount()).isEqualTo(filteredGraph.nodeCount());

        assertThat(Arrays.stream(subgraph.originalNodeIds()))
            .allMatch(nodeId -> nodeId < filteredGraph.nodeCount());
    }
}
