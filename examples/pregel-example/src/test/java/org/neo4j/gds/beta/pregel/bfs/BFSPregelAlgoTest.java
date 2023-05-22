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
package org.neo4j.gds.beta.pregel.bfs;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class BFSPregelAlgoTest {

    @GdlGraph
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (i:Node)" +
        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (d)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";

    @GdlGraph(graphNamePrefix = "parent")
    private static final String PARENT_GRAPH =
        "CREATE" +
        "  (a) " +
        ", (b)" +
        ", (c)" +
        ", (d)" +
        ", (e)" +
        ", (f)" +
        ", (g)" +
        ", (h)" +
        ", (i)" +
        ", (j)" +
        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(d)" +
        ", (a)-[:TYPE]->(f)" +
        ", (b)-[:TYPE]->(a)" +
        ", (b)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(a)" +
        ", (c)-[:TYPE]->(b)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(a)" +
        ", (d)-[:TYPE]->(b)" +
        ", (d)-[:TYPE]->(c)" +
        ", (d)-[:TYPE]->(g)" +
        ", (d)-[:TYPE]->(i)" +
        ", (e)-[:TYPE]->(b)" +
        ", (f)-[:TYPE]->(b)" +
        ", (i)-[:TYPE]->(a)" +
        ", (i)-[:TYPE]->(b)" +
        ", (i)-[:TYPE]->(d)" +
        ", (j)-[:TYPE]->(b)" +
        ", (j)-[:TYPE]->(e)";

    @Inject
    private TestGraph graph;
    
    @Inject
    private TestGraph parentGraph;

    @Test
    void levelBfs() {
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(graph.toMappedNodeId("a"))
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new BFSLevelPregel(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(4, result.ranIterations());

       var expected = Map.of(
            "a", 0L,
            "b", 1L,
            "c", 1L,
            "d", 2L,
            "e", 3L,
            "f", 3L,
            "g", 4L,
            "i", -1L
        );

        TestSupport.assertLongValues(graph, (nodeId) -> result.nodeValues().longValue(BFSLevelPregel.LEVEL,nodeId), expected);
    }

    @Test
    void parentBfs() {
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(graph.toMappedNodeId("a"))
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new BFSParentPregel(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(4, result.ranIterations());

        var expected = Map.of(
            "a", graph.toMappedNodeId("a"),
            "b", graph.toMappedNodeId("a"),
            "c", graph.toMappedNodeId("a"),
            "d", graph.toMappedNodeId("b"),
            "e", graph.toMappedNodeId("d"),
            "f", graph.toMappedNodeId("d"),
            "g", graph.toMappedNodeId("e"),
            "i", Long.MAX_VALUE
        );

        TestSupport.assertLongValues(graph, (nodeId) -> result.nodeValues().longValue(BFSParentPregel.PARENT,nodeId), expected);
    }

    @Test
    void parentBugTest() {
        int maxIterations = 10;

        var config = ImmutableBFSPregelConfig.builder()
            .maxIterations(maxIterations)
            .startNode(parentGraph.toMappedNodeId("a"))
            .build();

        var pregelJob = Pregel.create(
            parentGraph,
            config,
            new BFSParentPregel(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var expected = Map.of(
            "a", parentGraph.toMappedNodeId("a"),
            "b", parentGraph.toMappedNodeId("a"),
            "c", parentGraph.toMappedNodeId("b"),
            "d", parentGraph.toMappedNodeId("a"),
            "e", BFSParentPregel.NOT_FOUND,
            "f", parentGraph.toMappedNodeId("a"),
            "g", parentGraph.toMappedNodeId("d"),
            "h", BFSParentPregel.NOT_FOUND,
            "i", parentGraph.toMappedNodeId("d"),
            "j", BFSParentPregel.NOT_FOUND
        );

        var result = pregelJob.run();

        assertTrue(result.didConverge(), "Algorithm did not converge.");
        assertEquals(3, result.ranIterations());

        TestSupport.assertLongValues(parentGraph, (nodeId) -> result.nodeValues().longValue(BFSParentPregel.PARENT,nodeId), expected);
    }
}
