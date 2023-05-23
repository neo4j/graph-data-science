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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

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
            TerminationFlag.RUNNING_TRUE
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("b"),
            0.42
        )));

        assertEquals(graph.nodeCount(), simGraph.nodeCount());
        assertEquals(1, simGraph.relationshipCount());

        assertGraphEquals(fromGdl("(a)-[:REL {w: 0.42000D}]->(b), (i1), (i2)"), simGraph);
    }

    @Test
    void testConstructionFromUnionGraph() {
        assertEquals(UnionGraph.class, graph.innerGraph().getClass());

        SimilarityGraphBuilder similarityGraphBuilder = new SimilarityGraphBuilder(
            graph,
            1,
            Pools.DEFAULT,
            TerminationFlag.RUNNING_TRUE
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(
            graph.toMappedNodeId("a"),
            graph.toMappedNodeId("b"),
            0.42
        )));

        assertEquals(graph.nodeCount(), simGraph.nodeCount());
        assertEquals(1, simGraph.relationshipCount());

        assertGraphEquals(fromGdl("(a:Person)-[:REL {w: 0.42000D}]->(b:Person), (i1:Item), (i2:Item)"), simGraph);
    }

    @Test
    void testConstructFromFilteredGraph() {
        NodesBuilder nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(4)
            .hasLabelInformation(true)
            .maxOriginalId(3)
            .build();

        nodesBuilder.addNode(0, NodeLabel.of("A"));
        nodesBuilder.addNode(1, NodeLabel.of("A"));
        nodesBuilder.addNode(2, NodeLabel.of("B"));
        nodesBuilder.addNode(3, NodeLabel.of("B"));

        var inputMapping = nodesBuilder.build().idMap();
        var filteredIdMap = inputMapping.withFilteredLabels(NodeLabel.listOf("B"), 4).get();

        SimilarityGraphBuilder similarityGraphBuilder = new SimilarityGraphBuilder(
            filteredIdMap,
            1,
            Pools.DEFAULT,
            TerminationFlag.RUNNING_TRUE
        );

        Graph simGraph = similarityGraphBuilder.build(Stream.of(new SimilarityResult(
            0, 1, 0.42
        )));

        assertGraphEquals(fromGdl("(a:A), (b:A), (c:B), (d:B), (c)-[:REL {similarity: 0.42}]->(d)"), simGraph);
    }



}
