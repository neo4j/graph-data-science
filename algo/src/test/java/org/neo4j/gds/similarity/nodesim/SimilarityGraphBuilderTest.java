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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

@GdlExtension
class SimilarityGraphBuilderTest {

    @GdlGraph
    private static final String DB =
        "CREATE" +
        "  (a:Person)-[:LIKES]->(i1:Item)" +
        ", (a)-[:LIKES]->(i2:Item)" +
        ", (b:Person)-[:LIKES]->(i1)" +
        ", (b)-[:LOVES]->(i2)";

    @GdlGraph(graphNamePrefix = "unlabelled")
    private static final String DB_UNLABELLED =
        "CREATE" +
        "  (a)-[:REL]->(i1)" +
        ", (a)-[:REL]->(i2)" +
        ", (b)-[:REL]->(i1)" +
        ", (b)-[:REL]->(i2)";

    @Inject
    private TestGraph graph;

    @Inject
    private TestGraph unlabelledGraph;

    @Test
    void testConstructionFromHugeGraph() {
        assertEquals(HugeGraph.class, unlabelledGraph.innerGraph().getClass());

        SimilarityGraphBuilder similarityGraphBuilder = new SimilarityGraphBuilder(
            unlabelledGraph,
            1,
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("b"),
            0.42
        )));

        assertEquals(graph.nodeCount(), simGraph.nodeCount());
        assertEquals(1, simGraph.relationshipCount());

        assertGraphEquals(fromGdl("(a)-[{w: 0.42000D}]->(b), (i1), (i2)"), simGraph);
    }

    @Test
    void testConstructionFromUnionGraph() {
        assertEquals(UnionGraph.class, graph.innerGraph().getClass());

        SimilarityGraphBuilder similarityGraphBuilder = new SimilarityGraphBuilder(
            graph,
            1,
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("b"),
            0.42
        )));

        assertEquals(graph.nodeCount(), simGraph.nodeCount());
        assertEquals(1, simGraph.relationshipCount());

        assertGraphEquals(fromGdl("(a:Person)-[{w: 0.42000D}]->(b:Person), (i1:Item), (i2:Item)"), simGraph);
    }
}
